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

#include "iceberg/cli/cli_app.h"

#include <format>
#include <iostream>
#include <map>
#include <span>
#include <utility>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <parquet/arrow/writer.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_io_util.h"
#include "iceberg/arrow/arrow_register.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/catalog/memory/in_memory_catalog.h"
#include "iceberg/constants.h"
#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_scan.h"
#include "iceberg/transaction.h"
#include "iceberg/type.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg::cli {
namespace {

std::string TableNameForPath(const TableIdentifier& table) {
  auto out = table.name;
  for (const auto& level : table.ns.levels) {
    out = level + "_" + out;
  }
  return out;
}

std::shared_ptr<::arrow::DataType> ToArrowType(const Type& type) {
  switch (type.type_id()) {
    case TypeId::kBoolean:
      return ::arrow::boolean();
    case TypeId::kInt:
      return ::arrow::int32();
    case TypeId::kLong:
      return ::arrow::int64();
    case TypeId::kFloat:
      return ::arrow::float32();
    case TypeId::kDouble:
      return ::arrow::float64();
    case TypeId::kString:
      return ::arrow::utf8();
    default:
      return nullptr;
  }
}

::arrow::Status AppendValue(::arrow::ArrayBuilder& builder, const SqlValue& value,
                            const Type& type) {
  if (std::holds_alternative<std::nullptr_t>(value)) {
    return builder.AppendNull();
  }
  switch (type.type_id()) {
    case TypeId::kBoolean:
      if (auto typed = std::get_if<bool>(&value)) {
        return static_cast<::arrow::BooleanBuilder&>(builder).Append(*typed);
      }
      break;
    case TypeId::kInt:
      if (auto typed = std::get_if<int64_t>(&value)) {
        return static_cast<::arrow::Int32Builder&>(builder).Append(
            static_cast<int32_t>(*typed));
      }
      break;
    case TypeId::kLong:
      if (auto typed = std::get_if<int64_t>(&value)) {
        return static_cast<::arrow::Int64Builder&>(builder).Append(*typed);
      }
      break;
    case TypeId::kFloat:
      if (auto typed = std::get_if<double>(&value)) {
        return static_cast<::arrow::FloatBuilder&>(builder).Append(
            static_cast<float>(*typed));
      }
      if (auto typed = std::get_if<int64_t>(&value)) {
        return static_cast<::arrow::FloatBuilder&>(builder).Append(
            static_cast<float>(*typed));
      }
      break;
    case TypeId::kDouble:
      if (auto typed = std::get_if<double>(&value)) {
        return static_cast<::arrow::DoubleBuilder&>(builder).Append(*typed);
      }
      if (auto typed = std::get_if<int64_t>(&value)) {
        return static_cast<::arrow::DoubleBuilder&>(builder).Append(
            static_cast<double>(*typed));
      }
      break;
    case TypeId::kString:
      if (auto typed = std::get_if<std::string>(&value)) {
        return static_cast<::arrow::StringBuilder&>(builder).Append(*typed);
      }
      break;
    default:
      break;
  }
  return ::arrow::Status::Invalid("Value does not match column type ", type.ToString());
}

}  // namespace

CliApp::CliApp(std::filesystem::path warehouse)
    : CliApp(std::move(warehouse), iceberg::arrow::MakeLocalFileIO(), nullptr,
             std::cout) {}

CliApp::CliApp(std::filesystem::path warehouse, std::shared_ptr<FileIO> file_io,
               std::shared_ptr<InMemoryCatalog> catalog, std::ostream& output)
    : warehouse_(std::move(warehouse)),
      file_io_(std::move(file_io)),
      catalog_(std::move(catalog)),
      output_(output) {
  if (catalog_ == nullptr) {
    catalog_ = InMemoryCatalog::Make("iceberg_cli", file_io_, warehouse_.string(), {});
  }
  iceberg::arrow::RegisterAll();
  iceberg::avro::RegisterAll();
  iceberg::parquet::RegisterAll();
}

Status CliApp::RunCommand(const Command& command) {
  return std::visit([this](const auto& typed) { return Execute(typed); }, command.value);
}

Status CliApp::Execute(const CreateTableCommand& command) {
  std::filesystem::path table_location = warehouse_ / TableNameForPath(command.table);
  std::error_code ec;
  std::filesystem::create_directories(table_location / "metadata", ec);
  std::filesystem::create_directories(table_location / "data", ec);
  if (ec) {
    return IOError("Failed to create table directory '{}': {}", table_location.string(),
                   ec.message());
  }

  std::vector<SchemaField> fields;
  int32_t field_id = 1;
  fields.reserve(command.columns.size());
  for (const auto& column : command.columns) {
    if (column.nullable) {
      fields.push_back(SchemaField::MakeOptional(field_id, column.name, column.type));
    } else {
      fields.push_back(SchemaField::MakeRequired(field_id, column.name, column.type));
    }
    ++field_id;
  }

  auto schema = std::make_shared<Schema>(std::move(fields), Schema::kInitialSchemaId);
  ICEBERG_ASSIGN_OR_RAISE(
      auto table,
      catalog_->CreateTable(command.table, schema, PartitionSpec::Unpartitioned(),
                            SortOrder::Unsorted(), table_location.string(), {}));
  tables_[command.table.ToString()] = table;
  output_ << "created table " << command.table.ToString() << "\n";
  return {};
}

Status CliApp::Execute(const InsertCommand& command) {
  ICEBERG_ASSIGN_OR_RAISE(auto table, LoadTable(command.table));
  ICEBERG_ASSIGN_OR_RAISE(auto schema, table->schema());

  std::vector<std::string> column_names = command.columns;
  if (column_names.empty()) {
    for (const auto& field : schema->fields()) {
      column_names.push_back(std::string(field.name()));
    }
  }
  if (command.rows.empty()) {
    return InvalidArgument("INSERT has no rows");
  }
  for (const auto& row : command.rows) {
    if (row.size() != column_names.size()) {
      return InvalidArgument("INSERT row has {} values but {} columns were specified",
                             row.size(), column_names.size());
    }
  }

  std::map<int32_t, std::vector<SqlValue>> values_by_field_id;
  for (size_t column_index = 0; column_index < column_names.size(); ++column_index) {
    ICEBERG_ASSIGN_OR_RAISE(auto field_opt,
                            schema->FindFieldByName(column_names[column_index]));
    if (!field_opt.has_value()) {
      return InvalidArgument("Unknown column '{}'", column_names[column_index]);
    }
    const auto& field = field_opt->get();
    std::vector<SqlValue> values;
    values.reserve(command.rows.size());
    for (const auto& row : command.rows) {
      values.push_back(row[column_index]);
    }
    values_by_field_id.emplace(field.field_id(), std::move(values));
  }

  std::vector<std::shared_ptr<::arrow::Field>> arrow_fields;
  std::vector<std::shared_ptr<::arrow::Array>> arrays;
  arrow_fields.reserve(schema->fields().size());
  arrays.reserve(schema->fields().size());
  for (const auto& field : schema->fields()) {
    auto arrow_type = ToArrowType(*field.type());
    if (arrow_type == nullptr) {
      return NotSupported("Unsupported column type '{}'", field.type()->ToString());
    }
    auto metadata = ::arrow::key_value_metadata({std::string(kParquetFieldIdKey)},
                                                {std::to_string(field.field_id())});
    arrow_fields.push_back(::arrow::field(std::string(field.name()), arrow_type,
                                          field.optional(), metadata));

    auto builder_result =
        ::arrow::MakeBuilder(arrow_type, ::arrow::default_memory_pool());
    if (!builder_result.ok()) {
      return IOError("Failed to create Arrow builder for column '{}': {}", field.name(),
                     builder_result.status().ToString());
    }
    auto builder = std::move(builder_result).ValueOrDie();
    auto values_it = values_by_field_id.find(field.field_id());
    for (size_t row_index = 0; row_index < command.rows.size(); ++row_index) {
      if (values_it == values_by_field_id.end()) {
        if (!field.optional()) {
          return InvalidArgument("Required column '{}' has no value", field.name());
        }
        auto append_status = builder->AppendNull();
        if (!append_status.ok()) {
          return IOError("Failed to append NULL to column '{}': {}", field.name(),
                         append_status.ToString());
        }
        continue;
      }
      auto append_status =
          AppendValue(*builder, values_it->second[row_index], *field.type());
      if (!append_status.ok()) {
        return InvalidArgument("Failed to append value to column '{}': {}", field.name(),
                               append_status.ToString());
      }
    }
    auto array_result = builder->Finish();
    if (!array_result.ok()) {
      return IOError("Failed to finish Arrow array for column '{}': {}", field.name(),
                     array_result.status().ToString());
    }
    arrays.push_back(std::move(array_result).ValueOrDie());
  }

  auto arrow_schema = ::arrow::schema(std::move(arrow_fields));
  auto arrow_table = ::arrow::Table::Make(arrow_schema, std::move(arrays));
  std::filesystem::path data_path =
      std::filesystem::path(table->location()) / "data" /
      std::format("insert-{}.parquet", Uuid::GenerateV7().ToString());

  ICEBERG_ASSIGN_OR_RAISE(auto output,
                          arrow::OpenArrowOutputStream(file_io_, data_path.string()));
  auto write_status = ::parquet::arrow::WriteTable(
      *arrow_table, ::arrow::default_memory_pool(), output, 1024);
  if (!write_status.ok()) {
    return IOError("Failed to write Parquet file '{}': {}", data_path.string(),
                   write_status.ToString());
  }
  auto close_status = output->Close();
  if (!close_status.ok()) {
    return IOError("Failed to close Parquet file '{}': {}", data_path.string(),
                   close_status.ToString());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto input_file, file_io_->NewInputFile(data_path.string()));
  ICEBERG_ASSIGN_OR_RAISE(auto file_size, input_file->Size());
  auto data_file = std::make_shared<DataFile>();
  data_file->content = DataFile::Content::kData;
  data_file->file_path = data_path.string();
  data_file->file_format = FileFormatType::kParquet;
  data_file->record_count = static_cast<int64_t>(command.rows.size());
  data_file->file_size_in_bytes = static_cast<int64_t>(file_size);
  data_file->partition_spec_id = 0;
  data_file->sort_order_id = 0;

  ICEBERG_ASSIGN_OR_RAISE(auto transaction, table->NewTransaction());
  ICEBERG_ASSIGN_OR_RAISE(auto append, transaction->NewFastAppend());
  append->AppendFile(data_file);
  ICEBERG_RETURN_UNEXPECTED(append->Commit());
  ICEBERG_ASSIGN_OR_RAISE(auto updated_table, transaction->Commit());
  tables_[command.table.ToString()] = std::move(updated_table);
  output_ << "inserted " << command.rows.size() << " rows into "
          << command.table.ToString() << "\n";
  return {};
}

Status CliApp::Execute(const SelectCommand& command) {
  ICEBERG_ASSIGN_OR_RAISE(auto table, LoadTable(command.table));
  ICEBERG_ASSIGN_OR_RAISE(auto schema, table->schema());
  ICEBERG_ASSIGN_OR_RAISE(auto projected_schema, ProjectSchema(schema, command.columns));
  ICEBERG_ASSIGN_OR_RAISE(auto builder, table->NewScan());
  builder->Project(projected_schema);
  ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
  ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFiles());

  FileScanTaskReader::Options options{
      .io = file_io_,
      .table_schema = schema,
      .projected_schema = projected_schema,
  };
  ICEBERG_ASSIGN_OR_RAISE(auto reader, FileScanTaskReader::Make(std::move(options)));

  bool printed_header = false;
  int64_t row_count = 0;
  for (const auto& task : tasks) {
    ICEBERG_ASSIGN_OR_RAISE(auto stream, reader->Open(*task));
    auto maybe_reader = ::arrow::ImportRecordBatchReader(&stream);
    if (!maybe_reader.ok()) {
      return IOError("Failed to import Arrow stream: {}",
                     maybe_reader.status().ToString());
    }
    auto batch_reader = maybe_reader.ValueOrDie();
    while (true) {
      auto next = batch_reader->Next();
      if (!next.ok()) {
        return IOError("Failed to read Arrow batch: {}", next.status().ToString());
      }
      auto batch = next.ValueOrDie();
      if (batch == nullptr) {
        break;
      }
      PrintBatch(*batch, printed_header);
      printed_header = true;
      row_count += batch->num_rows();
    }
  }
  output_ << "(" << row_count << " rows)\n";
  return {};
}

Status CliApp::Execute(const ExplainCommand& command) {
  ICEBERG_ASSIGN_OR_RAISE(auto table, LoadTable(command.select.table));
  ICEBERG_ASSIGN_OR_RAISE(auto schema, table->schema());
  ICEBERG_ASSIGN_OR_RAISE(auto projected_schema,
                          ProjectSchema(schema, command.select.columns));
  ICEBERG_ASSIGN_OR_RAISE(auto builder, table->NewScan());
  builder->Project(projected_schema);
  ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
  ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFiles());

  output_ << "ScanTasks: " << tasks.size() << "\n";
  for (size_t index = 0; index < tasks.size(); ++index) {
    const auto& task = tasks[index];
    const auto& data_file = task->data_file();
    output_ << "  [" << index << "] file=" << data_file->file_path
            << " rows=" << task->estimated_row_count() << " bytes=" << task->size_bytes()
            << " delete_files=" << task->delete_files().size() << "\n";
  }
  return {};
}

Result<std::shared_ptr<Table>> CliApp::LoadTable(const TableIdentifier& table_id) {
  auto key = table_id.ToString();
  auto existing = tables_.find(key);
  if (existing != tables_.end()) {
    return existing->second;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto table, catalog_->LoadTable(table_id));
  tables_[key] = table;
  return table;
}

Result<std::shared_ptr<Schema>> CliApp::ProjectSchema(
    const std::shared_ptr<Schema>& schema, const std::vector<std::string>& columns) {
  if (columns.empty() || (columns.size() == 1 && columns.front() == "*")) {
    return schema;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto projected, schema->Select(std::span(columns)));
  return std::shared_ptr<Schema>(std::move(projected));
}

void CliApp::PrintBatch(const ::arrow::RecordBatch& batch, bool printed_header) {
  if (!printed_header) {
    for (int column = 0; column < batch.num_columns(); ++column) {
      if (column != 0) {
        output_ << "\t";
      }
      output_ << batch.schema()->field(column)->name();
    }
    output_ << "\n";
  }
  for (int64_t row = 0; row < batch.num_rows(); ++row) {
    for (int column = 0; column < batch.num_columns(); ++column) {
      if (column != 0) {
        output_ << "\t";
      }
      auto scalar = batch.column(column)->GetScalar(row);
      if (!scalar.ok()) {
        output_ << "<error>";
      } else if (!scalar.ValueOrDie()->is_valid) {
        output_ << "NULL";
      } else {
        output_ << scalar.ValueOrDie()->ToString();
      }
    }
    output_ << "\n";
  }
}

}  // namespace iceberg::cli
