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

#include <algorithm>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/api.h>
#include <arrow/extension/parquet_variant.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <parquet/exception.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/parquet/parquet_variant_util_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

#if ICEBERG_HAS_ARROW_VARIANT

#  include <parquet/variant/builder.h>
#  include <parquet/variant/decoding.h>
#  include <parquet/variant/shred.h>
#  include <parquet/variant/unshred.h>
#  include <parquet/variant/validate.h>
#endif

namespace iceberg::parquet {

namespace {

[[maybe_unused]] unexpected<Error> MissingVariantSupport() {
  return NotSupported(
      "Parquet variant I/O requires Arrow's variant validation and "
      "shredding utilities (apache/arrow#50252)");
}

bool HasVariant(const Type& type) {
  if (type.is_variant()) return true;
  if (type.is_nested()) {
    for (const auto& field : static_cast<const NestedType&>(type).fields()) {
      if (HasVariant(*field.type())) return true;
    }
  }
  return false;
}

#if ICEBERG_HAS_ARROW_VARIANT

// Arrow's shredder builds binary-view columns, whose slicing is not yet
// supported by the Parquet writer. Use binary storage for the file schema.
std::shared_ptr<::arrow::DataType> MaterializeStorageType(
    const std::shared_ptr<::arrow::DataType>& type) {
  if (type->id() == ::arrow::Type::BINARY_VIEW) return ::arrow::binary();
  if (type->id() == ::arrow::Type::STRUCT) {
    ::arrow::FieldVector fields;
    for (const auto& field : type->fields()) {
      fields.push_back(field->WithType(MaterializeStorageType(field->type())));
    }
    return ::arrow::struct_(std::move(fields));
  }
  if (type->id() == ::arrow::Type::LIST) {
    return ::arrow::list(
        type->field(0)->WithType(MaterializeStorageType(type->field(0)->type())));
  }
  return type;
}

Result<std::shared_ptr<::arrow::Array>> CastVariantStorage(
    const ::arrow::extension::VariantArray& array,
    const std::shared_ptr<::arrow::DataType>& output_type) {
  const auto& extension = static_cast<const ::arrow::ExtensionType&>(*output_type);
  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto storage, ::arrow::compute::Cast(array.storage(), extension.storage_type()));
  return ::arrow::ExtensionType::WrapArray(output_type, storage.make_array());
}

// Shredding targets describe values, not Iceberg fields. Remove Iceberg IDs and
// other field metadata before handing their logical layout to Arrow.
std::shared_ptr<::arrow::DataType> StripTargetMetadata(
    const std::shared_ptr<::arrow::DataType>& type) {
  if (type->id() == ::arrow::Type::STRUCT) {
    ::arrow::FieldVector fields;
    for (const auto& field : type->fields()) {
      fields.push_back(::arrow::field(field->name(), StripTargetMetadata(field->type()),
                                      field->nullable()));
    }
    return ::arrow::struct_(std::move(fields));
  }
  if (type->id() == ::arrow::Type::LIST) {
    const auto& field = type->field(0);
    return ::arrow::list(::arrow::field(field->name(), StripTargetMetadata(field->type()),
                                        field->nullable()));
  }
  return type;
}

Status CheckTarget(const Type& type) {
  if (type.is_struct() || type.is_list()) {
    const auto& nested = static_cast<const NestedType&>(type);
    for (const auto& field : nested.fields()) {
      if (!field.type()) {
        return InvalidArgument("Variant shredding target contains a null type");
      }
      ICEBERG_RETURN_UNEXPECTED(CheckTarget(*field.type()));
    }
    return {};
  }
  if (!type.is_primitive() || type.type_id() == TypeId::kUnknown ||
      type.type_id() == TypeId::kGeometry || type.type_id() == TypeId::kGeography ||
      type.type_id() == TypeId::kFixed) {
    return InvalidArgument("Unsupported variant shredding target: {}", type);
  }
  return {};
}

Result<std::shared_ptr<::arrow::DataType>> TargetToArrow(
    const std::shared_ptr<Type>& type) {
  ICEBERG_RETURN_UNEXPECTED(CheckTarget(*type));
  Schema schema({SchemaField::MakeOptional(0, "target", type)});
  ArrowSchema c_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &c_schema));
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto arrow_schema, ::arrow::ImportSchema(&c_schema));
  return StripTargetMetadata(arrow_schema->field(0)->type());
}

void AddTargetNames(const Type& type, ::parquet::variant::VariantBuilder& builder) {
  if (type.is_nested()) {
    for (const auto& field : static_cast<const NestedType&>(type).fields()) {
      if (type.is_struct()) builder.AddFieldName(field.name());
      AddTargetNames(*field.type(), builder);
    }
  }
}

std::shared_ptr<::arrow::extension::VariantArray> PrepareVariant(
    const ::arrow::extension::VariantArray& array, const Type* target,
    const std::vector<uint8_t>* visible, ::arrow::MemoryPool* pool) {
  const auto& storage = static_cast<const ::arrow::StructArray&>(*array.storage());
  const auto& metadata = static_cast<const ::arrow::BinaryArray&>(*storage.field(0));
  const auto& values = static_cast<const ::arrow::BinaryArray&>(*storage.field(1));
  ::parquet::variant::VariantArrayBuilder output(pool);
  for (int64_t i = 0; i < array.length(); ++i) {
    if (array.IsNull(i) || (visible != nullptr && !(*visible)[i])) {
      // Required storage children use empty bytes for a null outer variant.
      output.AppendNull();
      continue;
    }
    if (metadata.IsNull(i) || values.IsNull(i)) {
      throw ::parquet::ParquetException("Non-null variant has null metadata or value");
    }
    if (target == nullptr) {
      output.AppendEncoded({::arrow::Buffer::FromString(std::string(metadata.GetView(i))),
                            ::arrow::Buffer::FromString(std::string(values.GetView(i)))});
      continue;
    }
    auto dictionary = ::parquet::variant::VariantMetadataView::Make(metadata.GetView(i));
    ::parquet::variant::VariantBuilder row(pool);
    // Preserve existing dictionary IDs so encoded object references stay valid.
    for (uint32_t id = 0; id < dictionary.dictionary_size(); ++id) {
      row.AddFieldName(dictionary.string(id));
    }
    AddTargetNames(*target, row);
    row.AppendEncodedValue(values.GetView(i));
    output.AppendEncoded(row.Finish());
  }
  return output.Finish();
}

#endif

Result<std::shared_ptr<::arrow::DataType>> MakeWriteType(
    const Type& type, int32_t field_id, const std::shared_ptr<::arrow::DataType>& input,
    const std::unordered_map<int32_t, std::shared_ptr<Type>>& targets,
    ::arrow::MemoryPool* pool) {
  if (type.is_variant()) {
    auto target = targets.find(field_id);
    if (target == targets.end()) {
      return input;
    }
#if ICEBERG_HAS_ARROW_VARIANT
    ICEBERG_ASSIGN_OR_RAISE(auto target_type, TargetToArrow(target->second));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto empty, ::arrow::MakeArrayOfNull(input, 0, pool));
    auto prepared =
        PrepareVariant(static_cast<const ::arrow::extension::VariantArray&>(*empty),
                       target->second.get(), nullptr, pool);
    auto shredded = ::parquet::variant::ShredVariantArray(*prepared, target_type, pool);
    return ::arrow::extension::variant(MaterializeStorageType(
        static_cast<const ::arrow::ExtensionType&>(*shredded->type()).storage_type()));
#else
    return MissingVariantSupport();
#endif
  }
  if (type.is_struct()) {
    ::arrow::FieldVector fields;
    const auto& iceberg_fields = static_cast<const StructType&>(type).fields();
    for (size_t i = 0; i < iceberg_fields.size(); ++i) {
      const auto& field = iceberg_fields[i];
      ICEBERG_ASSIGN_OR_RAISE(auto child,
                              MakeWriteType(*field.type(), field.field_id(),
                                            input->field(i)->type(), targets, pool));
      fields.push_back(input->field(i)->WithType(std::move(child)));
    }
    return ::arrow::struct_(std::move(fields));
  }
  if (type.is_list()) {
    const auto& element = static_cast<const ListType&>(type).element();
    ICEBERG_ASSIGN_OR_RAISE(auto child,
                            MakeWriteType(*element.type(), element.field_id(),
                                          input->field(0)->type(), targets, pool));
    return ::arrow::list(input->field(0)->WithType(std::move(child)));
  }
  if (type.is_map()) {
    const auto& map = static_cast<const MapType&>(type);
    const auto& arrow_map = static_cast<const ::arrow::MapType&>(*input);
    ICEBERG_ASSIGN_OR_RAISE(auto key,
                            MakeWriteType(*map.key().type(), map.key().field_id(),
                                          arrow_map.key_type(), targets, pool));
    ICEBERG_ASSIGN_OR_RAISE(auto value,
                            MakeWriteType(*map.value().type(), map.value().field_id(),
                                          arrow_map.item_type(), targets, pool));
    return std::make_shared<::arrow::MapType>(
        arrow_map.key_field()->WithType(std::move(key)),
        arrow_map.item_field()->WithType(std::move(value)), arrow_map.keys_sorted());
  }
  return input;
}

Result<std::shared_ptr<::arrow::Array>> ShredArray(
    const Type& type, int32_t field_id, bool optional,
    const std::shared_ptr<::arrow::Array>& array,
    const std::shared_ptr<::arrow::DataType>& output_type,
    const std::unordered_map<int32_t, std::shared_ptr<Type>>& targets,
    const std::vector<uint8_t>* visible, ::arrow::MemoryPool* pool) {
  if (!HasVariant(type)) {
    return array;
  }
  auto data = array->data()->Copy();
  if (type.is_variant()) {
    if (!optional) {
      for (int64_t i = 0; i < array->length(); ++i) {
        if (array->IsNull(i) && (visible == nullptr || (*visible)[i])) {
          return InvalidArgument("Required variant field {} contains a null", field_id);
        }
      }
    }
#if ICEBERG_HAS_ARROW_VARIANT
    auto target = targets.find(field_id);
    const auto& storage = static_cast<const ::arrow::StructArray&>(
        *static_cast<const ::arrow::extension::VariantArray&>(*array).storage());
    if (target == targets.end() && storage.field(0)->null_count() == 0 &&
        storage.field(1)->null_count() == 0 &&
        (visible == nullptr ||
         std::ranges::all_of(*visible, [](uint8_t valid) { return valid; }))) {
      return array;
    }
    auto prepared = PrepareVariant(
        static_cast<const ::arrow::extension::VariantArray&>(*array),
        target == targets.end() ? nullptr : target->second.get(), visible, pool);
    if (target == targets.end()) {
      return ProjectVariantArray(prepared, output_type, pool);
    }
    ICEBERG_ASSIGN_OR_RAISE(auto arrow_target, TargetToArrow(target->second));
    auto shredded = ::parquet::variant::ShredVariantArray(*prepared, arrow_target, pool);
    return CastVariantStorage(*shredded, output_type);
#else
    return MissingVariantSupport();
#endif
  }
  std::vector<uint8_t> child_visible;
  if (type.is_struct()) {
    const auto& fields = static_cast<const StructType&>(type).fields();
    for (size_t child_index = 0; child_index < fields.size(); ++child_index) {
      auto child = ::arrow::MakeArray(data->child_data[child_index]);
      child_visible.assign(child->length(), 0);
      for (int64_t i = 0; i < array->length(); ++i) {
        child_visible[array->offset() + i] =
            array->IsValid(i) && (visible == nullptr || (*visible)[i]);
      }
      ICEBERG_ASSIGN_OR_RAISE(
          auto transformed,
          ShredArray(*fields[child_index].type(), fields[child_index].field_id(),
                     fields[child_index].optional(), child,
                     output_type->field(child_index)->type(), targets, &child_visible,
                     pool));
      data->child_data[child_index] = transformed->data();
    }
  } else if (type.is_list() || type.is_map()) {
    const auto& list = static_cast<const ::arrow::ListArray&>(*array);
    auto child = ::arrow::MakeArray(data->child_data[0]);
    child_visible.assign(child->length(), 0);
    for (int64_t i = 0; i < array->length(); ++i) {
      if (array->IsValid(i) && (visible == nullptr || (*visible)[i])) {
        std::fill(child_visible.begin() + list.value_offset(i),
                  child_visible.begin() + list.value_offset(i + 1), 1);
      }
    }
    if (type.is_list()) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto transformed,
          ShredArray(*static_cast<const ListType&>(type).element().type(),
                     static_cast<const ListType&>(type).element().field_id(),
                     static_cast<const ListType&>(type).element().optional(), child,
                     output_type->field(0)->type(), targets, &child_visible, pool));
      data->child_data[0] = transformed->data();
    } else {
      const auto& map = static_cast<const MapType&>(type);
      StructType entries({map.key(), map.value()});
      ICEBERG_ASSIGN_OR_RAISE(
          auto transformed,
          ShredArray(entries, -1, true, child, output_type->field(0)->type(), targets,
                     &child_visible, pool));
      data->child_data[0] = transformed->data();
    }
  }
  data->type = output_type;
  return ::arrow::MakeArray(std::move(data));
}

}  // namespace

Status CheckVariantSupport(const Type& type) {
  if (type.is_variant()) {
#if ICEBERG_HAS_ARROW_VARIANT
    return {};
#else
    return MissingVariantSupport();
#endif
  }
  if (type.is_nested()) {
    for (const auto& field : static_cast<const NestedType&>(type).fields()) {
      ICEBERG_RETURN_UNEXPECTED(CheckVariantSupport(*field.type()));
    }
  }
  return {};
}

Result<std::shared_ptr<::arrow::Array>> ProjectVariantArray(
    const std::shared_ptr<::arrow::Array>& array,
    const std::shared_ptr<::arrow::DataType>& output_type, ::arrow::MemoryPool* pool) {
#if ICEBERG_HAS_ARROW_VARIANT
  try {
    const auto& variant = static_cast<const ::arrow::extension::VariantArray&>(*array);
    auto unshredded = ::parquet::variant::UnshredVariantArray(variant, pool);
    // Arrow may return binary-view storage. The C Data interface uses canonical
    // binary storage so that the schema stays the same across files and batches.
    return CastVariantStorage(*unshredded, output_type);
  } catch (const ::parquet::ParquetException& error) {
    return Invalid("Invalid Parquet variant: {}", error.what());
  }
#else
  return MissingVariantSupport();
#endif
}

Result<std::shared_ptr<::arrow::Schema>> MakeVariantWriteSchema(
    const Schema& schema, const std::shared_ptr<::arrow::Schema>& input_schema,
    const std::unordered_map<int32_t, std::shared_ptr<Type>>& targets,
    ::arrow::MemoryPool* pool) {
  ICEBERG_RETURN_UNEXPECTED(CheckVariantSupport(schema));
  for (const auto& [field_id, target] : targets) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema.FindFieldById(field_id));
    if (!field || !field->get().type()->is_variant()) {
      return InvalidArgument("Variant shredding field id {} does not identify a variant",
                             field_id);
    }
    if (!target) {
      return InvalidArgument("Variant shredding target for field id {} is null",
                             field_id);
    }
  }
  try {
    ICEBERG_ASSIGN_OR_RAISE(
        auto type, MakeWriteType(schema, -1, ::arrow::struct_(input_schema->fields()),
                                 targets, pool));
    return ::arrow::schema(type->fields(), input_schema->metadata());
  } catch (const ::parquet::ParquetException& error) {
    return InvalidArgument("Invalid variant shredding target: {}", error.what());
  }
}

Result<std::shared_ptr<::arrow::RecordBatch>> ShredVariantBatch(
    const Schema& schema, const std::shared_ptr<::arrow::RecordBatch>& batch,
    const std::shared_ptr<::arrow::Schema>& output_schema,
    const std::unordered_map<int32_t, std::shared_ptr<Type>>& targets,
    ::arrow::MemoryPool* pool) {
  try {
    if (!HasVariant(schema)) return batch;
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto array, batch->ToStructArray());
#if ICEBERG_HAS_ARROW_VARIANT
    if (!targets.empty()) {
      ::parquet::variant::ValidateVariants<true>(::arrow::ChunkedArray(array), pool);
    }
#endif
    ICEBERG_ASSIGN_OR_RAISE(
        auto output,
        ShredArray(schema, -1, false, array, ::arrow::struct_(output_schema->fields()),
                   targets, nullptr, pool));
    const auto& structure = static_cast<const ::arrow::StructArray&>(*output);
    return ::arrow::RecordBatch::Make(output_schema, batch->num_rows(),
                                      structure.fields());
  } catch (const ::parquet::ParquetException& error) {
    return Invalid("Cannot shred variant batch: {}", error.what());
  }
}

}  // namespace iceberg::parquet
