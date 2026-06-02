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

#pragma once

#include <filesystem>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/cli/command.h"
#include "iceberg/result.h"

namespace arrow {
class RecordBatch;
}  // namespace arrow

namespace iceberg {
class FileIO;
class InMemoryCatalog;
class Schema;
class Table;
}  // namespace iceberg

namespace iceberg::cli {

class CliApp {
 public:
  explicit CliApp(std::filesystem::path warehouse);
  CliApp(std::filesystem::path warehouse, std::shared_ptr<FileIO> file_io,
         std::shared_ptr<InMemoryCatalog> catalog, std::ostream& output);

  Status RunCommand(const Command& command);

 private:
  Status Execute(const CreateTableCommand& command);
  Status Execute(const InsertCommand& command);
  Status Execute(const SelectCommand& command);
  Status Execute(const ExplainCommand& command);

  Result<std::shared_ptr<Table>> LoadTable(const TableIdentifier& table_id);
  Result<std::shared_ptr<Schema>> ProjectSchema(const std::shared_ptr<Schema>& schema,
                                                const std::vector<std::string>& columns);
  void PrintBatch(const ::arrow::RecordBatch& batch, bool printed_header);

  std::filesystem::path warehouse_;
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<InMemoryCatalog> catalog_;
  std::ostream& output_;
  std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
};

}  // namespace iceberg::cli
