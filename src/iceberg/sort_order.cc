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

#include "iceberg/sort_order.h"

#include <format>
#include <memory>
#include <optional>
#include <ranges>

#include "iceberg/exception.h"
#include "iceberg/expression/term.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/sort_field.h"
#include "iceberg/transform.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/macros.h"

namespace iceberg {

SortOrder::SortOrder(int32_t order_id, std::vector<SortField> fields)
    : order_id_(order_id), fields_(std::move(fields)) {}

const std::shared_ptr<SortOrder>& SortOrder::Unsorted() {
  static const std::shared_ptr<SortOrder> unsorted =
      std::make_shared<SortOrder>(kUnsortedOrderId, std::vector<SortField>{});
  return unsorted;
}

int32_t SortOrder::order_id() const { return order_id_; }

std::span<const SortField> SortOrder::fields() const { return fields_; }

bool SortOrder::Satisfies(const SortOrder& other) const {
  // any ordering satisfies an unsorted ordering
  if (other.is_unsorted()) {
    return true;
  }

  // this ordering cannot satisfy an ordering with more sort fields
  if (fields_.size() < other.fields().size()) {
    return false;
  }

  // this ordering has either more or the same number of sort fields
  for (const auto& [field, other_field] : std::views::zip(fields_, other.fields_)) {
    if (!field.Satisfies(other_field)) {
      return false;
    }
  }

  return true;
}

bool SortOrder::SameOrder(const SortOrder& other) const {
  return fields_ == other.fields_;
}

std::string SortOrder::ToString() const {
  std::string repr = "[";
  for (const auto& field : fields_) {
    std::format_to(std::back_inserter(repr), "\n  {}", field);
  }
  if (!fields_.empty()) {
    repr.push_back('\n');
  }
  repr += "]";
  return repr;
}

bool SortOrder::Equals(const SortOrder& other) const {
  return order_id_ == other.order_id_ && fields_ == other.fields_;
}

// SortOrderBuilder implementation

struct SortOrderBuilder::Impl {
  const Schema* schema;
  std::optional<int32_t> sort_id;
  std::vector<SortField> fields;
  bool case_sensitive{false};

  explicit Impl(const Schema* schema) : schema(schema) {}
};

SortOrderBuilder::~SortOrderBuilder() = default;

SortOrderBuilder::SortOrderBuilder(SortOrderBuilder&&) noexcept = default;

SortOrderBuilder& SortOrderBuilder::operator=(SortOrderBuilder&&) noexcept = default;

SortOrderBuilder::SortOrderBuilder(const Schema* schema)
    : impl_(std::make_unique<Impl>(schema)) {}

std::unique_ptr<SortOrderBuilder> SortOrderBuilder::BuildFromSchema(
    const Schema* schema) {
  return std::unique_ptr<SortOrderBuilder>(new SortOrderBuilder(schema));  // NOLINT
}

SortOrderBuilder& SortOrderBuilder::WithOrderId(int32_t sort_id) {
  impl_->sort_id = sort_id;
  return *this;
}

SortOrderBuilder& SortOrderBuilder::CaseSensitive(bool case_sensitive) {
  impl_->case_sensitive = case_sensitive;
  return *this;
}

Result<std::unique_ptr<SortOrder>> SortOrderBuilder::BuildUncheckd() {
  if (impl_->fields.empty()) {
    if (impl_->sort_id.has_value() && impl_->sort_id != SortOrder::kUnsortedOrderId) {
      return InvalidArgument("Unsorted order ID must be 0");
    }
    return std::make_unique<SortOrder>(SortOrder::kUnsortedOrderId,
                                       std::vector<SortField>{});
  }

  if (impl_->sort_id.has_value() && impl_->sort_id == SortOrder::kUnsortedOrderId) {
    return InvalidArgument("Sort order ID 0 is reserved for unsorted order");
  }

  // default ID to 1 as 0 is reserved for unsorted order
  return std::make_unique<SortOrder>(
      impl_->sort_id.value_or(SortOrder::kInitialSortOrderId), std::move(impl_->fields));
}

Result<std::unique_ptr<SortOrder>> SortOrderBuilder::Build() {
  ICEBERG_ASSIGN_OR_RAISE(auto sort_order, BuildUncheckd());
  ICEBERG_RETURN_UNEXPECTED(CheckCompatibility(sort_order, impl_->schema));
  return sort_order;
}

SortOrderBuilder& SortOrderBuilder::AddSortField(
    int32_t source_id, const std::shared_ptr<Transform>& transform,
    SortDirection direction, NullOrder null_order) {
  impl_->fields.emplace_back(source_id, transform, direction, null_order);
  return *this;
}

SortOrderBuilder& SortOrderBuilder::AddSortField(const std::shared_ptr<Term>& term,
                                                 SortDirection direction,
                                                 NullOrder null_order) {
  if (auto named_ref = std::dynamic_pointer_cast<NamedReference>(term)) {
    auto bound_ref = named_ref->Bind(*impl_->schema, impl_->case_sensitive);
    ICEBERG_CHECK(bound_ref.has_value(), "Failed to bind named reference to schema.");
    int32_t source_id = bound_ref.value()->field().field_id();
    impl_->fields.emplace_back(source_id, Transform::Identity(), direction, null_order);
  } else if (auto unbound_transform = std::dynamic_pointer_cast<UnboundTransform>(term)) {
    auto bound_transform = unbound_transform->Bind(*impl_->schema, impl_->case_sensitive);
    ICEBERG_CHECK(bound_transform.has_value(),
                  "Failed to bind unbound transform to schema.");
    int32_t source_id = bound_transform.value()->reference()->field().field_id();
    impl_->fields.emplace_back(source_id, bound_transform.value()->transform(), direction,
                               null_order);
  } else {
    throw IcebergError(std::format(
        "Invalid term: {}, expected either a named reference or an unbound transform",
        term ? term->ToString() : "null"));
  }

  return *this;
}

Status SortOrderBuilder::CheckCompatibility(const std::unique_ptr<SortOrder>& sort_order,
                                            const Schema* schema) {
  for (const auto& field : sort_order->fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema_field, schema->FindFieldById(field.source_id()));
    if (schema_field == std::nullopt) {
      return ValidationError("Cannot find source column for sort field: {}", field);
    }

    const auto& source_type = schema_field.value().get().type();

    if (!source_type->is_primitive()) {
      return ValidationError("Cannot sort by non-primitive source field: {}",
                             *source_type);
    }

    ICEBERG_RETURN_UNEXPECTED(field.transform()->ResultType(source_type));
  }
  return {};
}

}  // namespace iceberg
