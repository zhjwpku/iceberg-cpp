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

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg::cli {

struct ColumnDef {
  std::string name;
  std::shared_ptr<Type> type;
  bool nullable = true;
};

using SqlValue = std::variant<std::nullptr_t, bool, int64_t, double, std::string>;

struct CreateTableCommand {
  TableIdentifier table;
  std::vector<ColumnDef> columns;
};

struct InsertCommand {
  TableIdentifier table;
  std::vector<std::string> columns;
  std::vector<std::vector<SqlValue>> rows;
};

struct SelectCommand {
  TableIdentifier table;
  std::vector<std::string> columns;
};

struct ExplainCommand {
  SelectCommand select;
};

struct Command {
  std::variant<CreateTableCommand, InsertCommand, SelectCommand, ExplainCommand> value;
};

}  // namespace iceberg::cli
