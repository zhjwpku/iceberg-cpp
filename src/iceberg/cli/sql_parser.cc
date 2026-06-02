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

#include "iceberg/cli/sql_parser.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

extern "C" {
#include <pg_query.h>
}

#include "iceberg/cli/string_util.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg::cli {
namespace {

using Json = nlohmann::json;

const Json* NodePayload(const Json& node, std::string_view tag) {
  if (!node.is_object()) {
    return nullptr;
  }
  auto it = node.find(std::string(tag));
  if (it == node.end()) {
    return nullptr;
  }
  return &*it;
}

std::optional<std::string> StringNodeValue(const Json& node) {
  if (node.is_string()) {
    return node.get<std::string>();
  }
  if (auto payload = NodePayload(node, "String")) {
    auto it = payload->find("sval");
    if (it != payload->end() && it->is_string()) {
      return it->get<std::string>();
    }
  }
  return std::nullopt;
}

Result<TableIdentifier> ParseRangeVar(const Json& range_var_node) {
  const Json* range_var = NodePayload(range_var_node, "RangeVar");
  if (range_var == nullptr && range_var_node.is_object() &&
      range_var_node.contains("relname")) {
    range_var = &range_var_node;
  }
  if (range_var == nullptr) {
    return InvalidArgument("Expected RangeVar in SQL parse tree");
  }

  TableIdentifier table;
  if (auto it = range_var->find("schemaname");
      it != range_var->end() && it->is_string()) {
    table.ns.levels.push_back(it->get<std::string>());
  }
  auto relname = range_var->find("relname");
  if (relname == range_var->end() || !relname->is_string()) {
    return InvalidArgument("SQL table name is missing");
  }
  table.name = relname->get<std::string>();
  return table;
}

std::vector<std::string> TypeNameParts(const Json& type_name_node) {
  std::vector<std::string> parts;
  const Json* type_name = NodePayload(type_name_node, "TypeName");
  if (type_name == nullptr && type_name_node.is_object() &&
      type_name_node.contains("names")) {
    type_name = &type_name_node;
  }
  if (type_name == nullptr) {
    return parts;
  }
  auto names = type_name->find("names");
  if (names == type_name->end() || !names->is_array()) {
    return parts;
  }
  for (const auto& name_node : *names) {
    if (auto value = StringNodeValue(name_node)) {
      parts.push_back(Lower(*value));
    }
  }
  return parts;
}

Result<std::shared_ptr<Type>> ParseType(const Json& type_name_node) {
  auto parts = TypeNameParts(type_name_node);
  if (parts.empty()) {
    return InvalidArgument("Column type is missing");
  }
  const std::string& type_name = parts.back();
  if (type_name == "int" || type_name == "int4" || type_name == "integer") {
    return int32();
  }
  if (type_name == "bigint" || type_name == "int8" || type_name == "long") {
    return int64();
  }
  if (type_name == "real" || type_name == "float4") {
    return float32();
  }
  if (type_name == "double" || type_name == "float8" || type_name == "double precision") {
    return float64();
  }
  if (type_name == "text" || type_name == "varchar" || type_name == "bpchar" ||
      type_name == "string") {
    return string();
  }
  if (type_name == "bool" || type_name == "boolean") {
    return boolean();
  }
  return NotSupported("Unsupported SQL type '{}'", type_name);
}

bool HasNotNullConstraint(const Json& column_def) {
  auto constraints = column_def.find("constraints");
  if (constraints == column_def.end() || !constraints->is_array()) {
    return false;
  }
  for (const auto& constraint_node : *constraints) {
    const Json* constraint = NodePayload(constraint_node, "Constraint");
    if (constraint == nullptr) {
      continue;
    }
    auto contype = constraint->find("contype");
    if (contype != constraint->end() && contype->is_string() &&
        contype->get<std::string>() == "CONSTR_NOTNULL") {
      return true;
    }
  }
  return false;
}

Result<CreateTableCommand> ParseCreateTable(const Json& stmt) {
  const Json* create_stmt = NodePayload(stmt, "CreateStmt");
  if (create_stmt == nullptr) {
    return InvalidArgument("Expected CreateStmt");
  }

  auto relation = create_stmt->find("relation");
  if (relation == create_stmt->end()) {
    return InvalidArgument("CREATE TABLE relation is missing");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table, ParseRangeVar(*relation));

  auto table_elts = create_stmt->find("tableElts");
  if (table_elts == create_stmt->end() || !table_elts->is_array() ||
      table_elts->empty()) {
    return InvalidArgument("CREATE TABLE requires at least one column");
  }

  std::vector<ColumnDef> columns;
  for (const auto& table_elt : *table_elts) {
    const Json* column_def = NodePayload(table_elt, "ColumnDef");
    if (column_def == nullptr) {
      return NotSupported("CREATE TABLE only supports column definitions");
    }
    auto colname = column_def->find("colname");
    auto type_name = column_def->find("typeName");
    if (colname == column_def->end() || !colname->is_string() ||
        type_name == column_def->end()) {
      return InvalidArgument("Invalid CREATE TABLE column definition");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto type, ParseType(*type_name));
    columns.push_back(ColumnDef{.name = colname->get<std::string>(),
                                .type = std::move(type),
                                .nullable = !HasNotNullConstraint(*column_def)});
  }
  return CreateTableCommand{.table = std::move(table), .columns = std::move(columns)};
}

Result<SqlValue> ParseAConst(const Json& node) {
  const Json* constant = NodePayload(node, "A_Const");
  if (constant == nullptr) {
    return NotSupported("INSERT VALUES only supports SQL constants");
  }
  if (auto it = constant->find("isnull");
      it != constant->end() && it->is_boolean() && it->get<bool>()) {
    return SqlValue{nullptr};
  }
  if (auto it = constant->find("ival"); it != constant->end()) {
    auto nested = it->find("ival");
    if (nested != it->end() && nested->is_number_integer()) {
      return SqlValue{nested->get<int64_t>()};
    }
  }
  if (auto it = constant->find("fval"); it != constant->end()) {
    auto nested = it->find("fval");
    if (nested != it->end() && nested->is_string()) {
      return SqlValue{std::stod(nested->get<std::string>())};
    }
  }
  if (auto it = constant->find("sval"); it != constant->end()) {
    auto nested = it->find("sval");
    if (nested != it->end() && nested->is_string()) {
      return SqlValue{nested->get<std::string>()};
    }
  }
  if (auto it = constant->find("boolval"); it != constant->end()) {
    auto nested = it->find("boolval");
    if (nested != it->end() && nested->is_boolean()) {
      return SqlValue{nested->get<bool>()};
    }
  }
  return NotSupported("Unsupported SQL constant");
}

Result<InsertCommand> ParseInsert(const Json& stmt) {
  const Json* insert_stmt = NodePayload(stmt, "InsertStmt");
  if (insert_stmt == nullptr) {
    return InvalidArgument("Expected InsertStmt");
  }

  auto relation = insert_stmt->find("relation");
  if (relation == insert_stmt->end()) {
    return InvalidArgument("INSERT relation is missing");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table, ParseRangeVar(*relation));

  std::vector<std::string> columns;
  if (auto cols = insert_stmt->find("cols");
      cols != insert_stmt->end() && cols->is_array()) {
    for (const auto& target_node : *cols) {
      const Json* target = NodePayload(target_node, "ResTarget");
      if (target == nullptr) {
        return InvalidArgument("Invalid INSERT target column");
      }
      auto name = target->find("name");
      if (name == target->end() || !name->is_string()) {
        return InvalidArgument("Invalid INSERT target column name");
      }
      columns.push_back(name->get<std::string>());
    }
  }

  auto select_stmt_it = insert_stmt->find("selectStmt");
  if (select_stmt_it == insert_stmt->end()) {
    return NotSupported("INSERT only supports VALUES");
  }
  const Json* select_stmt = NodePayload(*select_stmt_it, "SelectStmt");
  if (select_stmt == nullptr) {
    return NotSupported("INSERT only supports VALUES");
  }
  auto values_lists = select_stmt->find("valuesLists");
  if (values_lists == select_stmt->end() || !values_lists->is_array()) {
    return NotSupported("INSERT only supports VALUES");
  }

  std::vector<std::vector<SqlValue>> rows;
  for (const auto& row_node : *values_lists) {
    const Json* row_items = &row_node;
    if (const Json* list = NodePayload(row_node, "List")) {
      auto items = list->find("items");
      if (items == list->end()) {
        return InvalidArgument("Invalid VALUES row");
      }
      row_items = &*items;
    }
    if (!row_items->is_array()) {
      return InvalidArgument("Invalid VALUES row");
    }
    std::vector<SqlValue> row;
    for (const auto& value_node : *row_items) {
      ICEBERG_ASSIGN_OR_RAISE(auto value, ParseAConst(value_node));
      row.push_back(std::move(value));
    }
    rows.push_back(std::move(row));
  }
  return InsertCommand{
      .table = std::move(table), .columns = std::move(columns), .rows = std::move(rows)};
}

Result<std::vector<std::string>> ParseTargetList(const Json& select_stmt) {
  std::vector<std::string> columns;
  auto targets = select_stmt.find("targetList");
  if (targets == select_stmt.end() || !targets->is_array()) {
    columns.push_back("*");
    return columns;
  }
  for (const auto& target_node : *targets) {
    const Json* target = NodePayload(target_node, "ResTarget");
    if (target == nullptr) {
      return InvalidArgument("Invalid SELECT target");
    }
    auto val = target->find("val");
    if (val == target->end()) {
      return InvalidArgument("Invalid SELECT target");
    }
    const Json* column_ref = NodePayload(*val, "ColumnRef");
    if (column_ref == nullptr) {
      return NotSupported("SELECT only supports column references and *");
    }
    auto fields = column_ref->find("fields");
    if (fields == column_ref->end() || !fields->is_array() || fields->empty()) {
      return InvalidArgument("Invalid SELECT column reference");
    }
    if (NodePayload(fields->front(), "A_Star") != nullptr) {
      columns.push_back("*");
      continue;
    }
    if (fields->size() != 1) {
      return NotSupported("SELECT only supports unqualified column names");
    }
    auto name = StringNodeValue(fields->front());
    if (!name) {
      return InvalidArgument("Invalid SELECT column name");
    }
    columns.push_back(*name);
  }
  return columns;
}

Result<SelectCommand> ParseSelectPayload(const Json& select_stmt) {
  if (select_stmt.contains("whereClause")) {
    return NotSupported("SELECT WHERE is not implemented in iceberg_cli yet");
  }
  auto from_clause = select_stmt.find("fromClause");
  if (from_clause == select_stmt.end() || !from_clause->is_array() ||
      from_clause->size() != 1) {
    return NotSupported("SELECT requires exactly one table");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table, ParseRangeVar(from_clause->front()));
  ICEBERG_ASSIGN_OR_RAISE(auto columns, ParseTargetList(select_stmt));
  return SelectCommand{.table = std::move(table), .columns = std::move(columns)};
}

Result<SelectCommand> ParseSelect(const Json& stmt) {
  const Json* select_stmt = NodePayload(stmt, "SelectStmt");
  if (select_stmt == nullptr) {
    return InvalidArgument("Expected SelectStmt");
  }
  return ParseSelectPayload(*select_stmt);
}

Result<ExplainCommand> ParseExplain(const Json& stmt) {
  const Json* explain_stmt = NodePayload(stmt, "ExplainStmt");
  if (explain_stmt == nullptr) {
    return InvalidArgument("Expected ExplainStmt");
  }
  auto query = explain_stmt->find("query");
  if (query == explain_stmt->end()) {
    return InvalidArgument("EXPLAIN query is missing");
  }
  const Json* select_stmt = NodePayload(*query, "SelectStmt");
  if (select_stmt == nullptr) {
    return NotSupported("EXPLAIN only supports SELECT");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto select, ParseSelectPayload(*select_stmt));
  return ExplainCommand{.select = std::move(select)};
}

}  // namespace

Result<Command> ParseSql(std::string_view sql) {
  PgQueryParseResult result = pg_query_parse(std::string(sql).c_str());
  if (result.error != nullptr) {
    std::string message =
        result.error->message != nullptr ? result.error->message : "SQL parse error";
    pg_query_free_parse_result(result);
    return InvalidArgument("{}", message);
  }

  Json parse_tree;
  try {
    parse_tree = Json::parse(result.parse_tree);
  } catch (const std::exception& e) {
    pg_query_free_parse_result(result);
    return JsonParseError("libpg_query produced invalid JSON: {}", e.what());
  }
  pg_query_free_parse_result(result);

  auto stmts = parse_tree.find("stmts");
  if (stmts == parse_tree.end() || !stmts->is_array() || stmts->size() != 1) {
    return NotSupported("Only one SQL statement is supported at a time");
  }
  auto stmt_it = stmts->front().find("stmt");
  if (stmt_it == stmts->front().end()) {
    return InvalidArgument("SQL statement is missing from parse tree");
  }
  const Json& stmt = *stmt_it;
  if (stmt.contains("CreateStmt")) {
    ICEBERG_ASSIGN_OR_RAISE(auto command, ParseCreateTable(stmt));
    return Command{std::move(command)};
  }
  if (stmt.contains("InsertStmt")) {
    ICEBERG_ASSIGN_OR_RAISE(auto command, ParseInsert(stmt));
    return Command{std::move(command)};
  }
  if (stmt.contains("SelectStmt")) {
    ICEBERG_ASSIGN_OR_RAISE(auto command, ParseSelect(stmt));
    return Command{std::move(command)};
  }
  if (stmt.contains("ExplainStmt")) {
    ICEBERG_ASSIGN_OR_RAISE(auto command, ParseExplain(stmt));
    return Command{std::move(command)};
  }
  return NotSupported("Unsupported SQL statement");
}

}  // namespace iceberg::cli
