/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "iceberg/cli/isolation_runner.h"

#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/catalog/memory/in_memory_catalog.h"
#include "iceberg/cli/cli_app.h"
#include "iceberg/cli/runner.h"
#include "iceberg/cli/string_util.h"
#include "iceberg/util/macros.h"

namespace iceberg::cli {
namespace {

struct IsolationSpec {
  std::string setup_sql;
  std::string verify_sql;
  std::map<std::string, std::string> session_sql;
  std::vector<std::vector<std::string>> concurrent_groups;
};

enum class SectionKind {
  kNone,
  kSetup,
  kVerify,
  kSession,
};

void AppendSql(std::string& sql, std::string_view line) {
  sql.append(line);
  sql.push_back('\n');
}

std::vector<std::string> SplitWords(std::string_view value) {
  std::istringstream stream{std::string(value)};
  std::vector<std::string> words;
  std::string word;
  while (stream >> word) {
    words.push_back(std::move(word));
  }
  return words;
}

Result<IsolationSpec> ParseIsolationSpec(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) {
    return IOError("Failed to open isolation test file '{}'", path.string());
  }

  IsolationSpec spec;
  SectionKind section = SectionKind::kNone;
  std::string current_session;
  std::string line;
  size_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number;
    auto trimmed = Trim(line);
    if (trimmed.empty() || trimmed.rfind("#", 0) == 0 ||
        trimmed.rfind("-- RUN:", 0) == 0) {
      continue;
    }

    if (trimmed == "setup:") {
      section = SectionKind::kSetup;
      current_session.clear();
      continue;
    }
    if (trimmed == "verify:") {
      section = SectionKind::kVerify;
      current_session.clear();
      continue;
    }
    if (trimmed.rfind("session ", 0) == 0 && trimmed.ends_with(":")) {
      current_session = trimmed.substr(std::string_view("session ").size());
      current_session.pop_back();
      current_session = Trim(current_session);
      if (current_session.empty()) {
        return InvalidArgument("{}:{}: session name is empty", path.string(),
                               line_number);
      }
      spec.session_sql.try_emplace(current_session, "");
      section = SectionKind::kSession;
      continue;
    }
    if (trimmed.rfind("concurrent:", 0) == 0) {
      auto group = SplitWords(trimmed.substr(std::string_view("concurrent:").size()));
      if (group.empty()) {
        return InvalidArgument("{}:{}: concurrent group is empty", path.string(),
                               line_number);
      }
      spec.concurrent_groups.push_back(std::move(group));
      section = SectionKind::kNone;
      current_session.clear();
      continue;
    }

    switch (section) {
      case SectionKind::kSetup:
        AppendSql(spec.setup_sql, line);
        break;
      case SectionKind::kVerify:
        AppendSql(spec.verify_sql, line);
        break;
      case SectionKind::kSession:
        AppendSql(spec.session_sql[current_session], line);
        break;
      case SectionKind::kNone:
        return InvalidArgument("{}:{}: SQL must be inside setup, session, or verify",
                               path.string(), line_number);
    }
  }

  if (spec.concurrent_groups.empty()) {
    return InvalidArgument("{}: isolation test requires at least one concurrent group",
                           path.string());
  }
  if (Trim(spec.verify_sql).empty()) {
    return InvalidArgument("{}: isolation test requires a verify section", path.string());
  }
  return spec;
}

struct SessionResult {
  Status status = {};
  std::string output;
};

}  // namespace

Status RunIsolationFile(const std::filesystem::path& path,
                        const std::filesystem::path& warehouse, std::ostream& output) {
  ICEBERG_ASSIGN_OR_RAISE(auto spec, ParseIsolationSpec(path));

  std::shared_ptr<FileIO> file_io(iceberg::arrow::MakeLocalFileIO());
  auto catalog =
      InMemoryCatalog::Make("iceberg_cli_isolation", file_io, warehouse.string(), {});

  std::ostringstream setup_output;
  CliApp setup_app(warehouse, file_io, catalog, setup_output);
  ICEBERG_RETURN_UNEXPECTED(ExecuteSql(setup_app, spec.setup_sql));
  output << "setup: ok\n";

  for (const auto& group : spec.concurrent_groups) {
    output << "concurrent:";
    for (const auto& session : group) {
      output << " " << session;
      if (!spec.session_sql.contains(session)) {
        return InvalidArgument("Unknown isolation session '{}'", session);
      }
    }
    output << "\n";

    std::map<std::string, std::unique_ptr<SessionResult>> results;
    std::vector<std::thread> threads;
    for (const auto& session : group) {
      auto result = std::make_unique<SessionResult>();
      auto* result_ptr = result.get();
      results.emplace(session, std::move(result));
      threads.emplace_back([&, session, result_ptr]() {
        std::ostringstream session_output;
        CliApp session_app(warehouse, file_io, catalog, session_output);
        result_ptr->status = ExecuteSql(session_app, spec.session_sql.at(session));
        result_ptr->output = std::move(session_output).str();
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }

    for (const auto& session : group) {
      const auto& result = results.at(session);
      if (!result->status.has_value()) {
        output << session << ": error: " << result->status.error().message << "\n";
        if (!result->output.empty()) {
          output << result->output;
        }
        return std::unexpected(result->status.error());
      }
      output << session << ": ok\n";
    }
  }

  output << "verify:\n";
  CliApp verify_app(warehouse, file_io, catalog, output);
  return ExecuteSql(verify_app, spec.verify_sql);
}

}  // namespace iceberg::cli
