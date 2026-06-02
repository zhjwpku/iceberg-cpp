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

#include "iceberg/cli/runner.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

extern "C" {
#include <linenoise.h>
}

#include "iceberg/cli/cli_app.h"
#include "iceberg/cli/sql_parser.h"
#include "iceberg/cli/string_util.h"
#include "iceberg/util/macros.h"

namespace iceberg::cli {

Status ExecuteSql(CliApp& app, std::string_view sql) {
  for (const auto& statement : SplitStatements(sql)) {
    ICEBERG_ASSIGN_OR_RAISE(auto command, ParseSql(statement));
    ICEBERG_RETURN_UNEXPECTED(app.RunCommand(command));
  }
  return {};
}

Status RunFile(CliApp& app, const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) {
    return IOError("Failed to open SQL file '{}'", path.string());
  }
  std::string sql((std::istreambuf_iterator<char>(input)),
                  std::istreambuf_iterator<char>());
  return ExecuteSql(app, sql);
}

Status RunRepl(CliApp& app) {
  linenoiseHistorySetMaxLen(100);
  std::string pending;
  while (true) {
    char* line = linenoise(pending.empty() ? "iceberg> " : "     -> ");
    if (line == nullptr) {
      std::cout << "\n";
      break;
    }
    std::string input(line);
    std::free(line);
    auto trimmed = Trim(input);
    if (trimmed == "\\q" || Lower(trimmed) == "quit" || Lower(trimmed) == "exit") {
      break;
    }
    if (trimmed.empty()) {
      continue;
    }
    pending += input;
    pending.push_back('\n');
    if (input.find(';') == std::string::npos) {
      continue;
    }
    linenoiseHistoryAdd(pending.c_str());
    auto status = ExecuteSql(app, pending);
    if (!status.has_value()) {
      std::cerr << "error: " << status.error().message << "\n";
    }
    pending.clear();
  }
  return {};
}

}  // namespace iceberg::cli
