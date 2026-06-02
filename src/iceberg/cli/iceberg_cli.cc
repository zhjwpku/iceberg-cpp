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

#include <filesystem>
#include <iostream>
#include <optional>
#include <string_view>

#include "iceberg/cli/cli_app.h"
#include "iceberg/cli/isolation_runner.h"
#include "iceberg/cli/runner.h"

namespace iceberg::cli {

void PrintUsage(std::string_view program) {
  std::cerr << "Usage: " << program
            << " [--warehouse PATH] [--file SQL_FILE | --isolation ISOLATION_FILE]\n"
               "       "
            << program << " --help\n";
}

}  // namespace iceberg::cli

int main(int argc, char** argv) {
  std::filesystem::path warehouse =
      std::filesystem::temp_directory_path() / "iceberg_cli_warehouse";
  std::optional<std::filesystem::path> sql_file;
  std::optional<std::filesystem::path> isolation_file;

  for (int index = 1; index < argc; ++index) {
    std::string_view arg = argv[index];
    if (arg == "--help" || arg == "-h") {
      iceberg::cli::PrintUsage(argv[0]);
      return 0;
    }
    if (arg == "--warehouse" && index + 1 < argc) {
      warehouse = argv[++index];
      continue;
    }
    if (arg == "--file" && index + 1 < argc) {
      sql_file = std::filesystem::path(argv[++index]);
      continue;
    }
    if (arg == "--isolation" && index + 1 < argc) {
      isolation_file = std::filesystem::path(argv[++index]);
      continue;
    }
    std::cerr << "Unknown argument: " << arg << "\n";
    iceberg::cli::PrintUsage(argv[0]);
    return 2;
  }

  if (sql_file.has_value() && isolation_file.has_value()) {
    std::cerr << "error: --file and --isolation are mutually exclusive\n";
    iceberg::cli::PrintUsage(argv[0]);
    return 2;
  }

  std::error_code ec;
  std::filesystem::create_directories(warehouse, ec);
  if (ec) {
    std::cerr << "error: failed to create warehouse '" << warehouse.string()
              << "': " << ec.message() << "\n";
    return 1;
  }

  auto status = [&]() {
    if (isolation_file.has_value()) {
      return iceberg::cli::RunIsolationFile(*isolation_file, warehouse, std::cout);
    }
    iceberg::cli::CliApp app(warehouse);
    return sql_file ? iceberg::cli::RunFile(app, *sql_file) : iceberg::cli::RunRepl(app);
  }();
  if (!status.has_value()) {
    std::cerr << "error: " << status.error().message << "\n";
    return 1;
  }
  return 0;
}
