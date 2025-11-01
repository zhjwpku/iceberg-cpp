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

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/term.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/sort_field.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief A sort order for a table
///
/// A sort order is defined by a sort order id and a list of sort fields.
/// The order of the sort fields within the list defines the order in which the sort is
/// applied to the data.
class ICEBERG_EXPORT SortOrder : public util::Formattable {
 public:
  static constexpr int32_t kUnsortedOrderId = 0;
  static constexpr int32_t kInitialSortOrderId = 1;

  SortOrder(int32_t order_id, std::vector<SortField> fields);

  /// \brief Get an unsorted sort order singleton.
  static const std::shared_ptr<SortOrder>& Unsorted();

  /// \brief Get the sort order id.
  int32_t order_id() const;

  /// \brief Get the list of sort fields.
  std::span<const SortField> fields() const;

  /// \brief Returns true if the sort order is sorted
  bool is_sorted() const { return !fields_.empty(); }

  /// \brief Returns true if the sort order is unsorted
  /// A SortOrder is unsorted if it has no sort fields.
  bool is_unsorted() const { return fields_.empty(); }

  /// \brief Checks whether this order satisfies another order.
  bool Satisfies(const SortOrder& other) const;

  /// \brief Checks whether this order is equivalent to another order while ignoring the
  /// order id.
  bool SameOrder(const SortOrder& other) const;

  std::string ToString() const override;

  friend bool operator==(const SortOrder& lhs, const SortOrder& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two sort orders for equality.
  bool Equals(const SortOrder& other) const;

  int32_t order_id_;
  std::vector<SortField> fields_;
};

/// \brief A builder used to create valid SortOrder instances.
class ICEBERG_EXPORT SortOrderBuilder {
 public:
  /// \brief Create a builder for a new SortOrder
  ///
  /// \return A new SortOrderBuilder instance initialized with Schema
  static std::unique_ptr<SortOrderBuilder> BuildFromSchema(const Schema* schema);

  /// \brief Add an expression term to the sort, ascending with the given null order.
  SortOrderBuilder& Asc(const std::shared_ptr<Term>& term, NullOrder null_order) {
    return AddSortField(term, SortDirection::kAscending, null_order);
  }

  /// \brief Add an expression term to the sort, descending with the given null order.
  SortOrderBuilder& Desc(const std::shared_ptr<Term>& term, NullOrder null_order) {
    return AddSortField(term, SortDirection::kDescending, null_order);
  }

  /// \brief Add a sort field to the sort order.
  SortOrderBuilder& SortBy(std::string name, SortDirection direction,
                           NullOrder null_order) {
    return AddSortField(Expressions::Ref(std::move(name)), direction, null_order);
  }

  /// \brief Add a sort field to the sort order.
  SortOrderBuilder& SortBy(const std::shared_ptr<Term>& term, SortDirection direction,
                           NullOrder null_order) {
    return AddSortField(term, direction, null_order);
  }

  /// \brief Set sort id to the sort order.
  SortOrderBuilder& WithOrderId(int32_t sort_id);

  /// \brief Set case sensitive to the sort order.
  SortOrderBuilder& CaseSensitive(bool case_sensitive);

  /// \brief Add a sort field to the sort order with the specified source field ID,
  /// transform, direction, and null order.
  ///
  /// \param source_id The source field ID.
  /// \param transform The transform to apply to the field.
  /// \param direction The sort direction.
  /// \param null_order The null ordering behavior (e.g., nulls first or nulls last).
  SortOrderBuilder& AddSortField(int32_t source_id,
                                 const std::shared_ptr<Transform>& transform,
                                 SortDirection direction, NullOrder null_order);

  /// \brief Builds a SortOrder instance.
  ///
  /// \return A Result containing the constructed SortOrder or an error
  Result<std::unique_ptr<SortOrder>> Build();

  /// \brief Destructor
  ~SortOrderBuilder();

  // Delete copy operations (use BuildFromSchema to create a new builder)
  SortOrderBuilder(const SortOrderBuilder&) = delete;
  SortOrderBuilder& operator=(const SortOrderBuilder&) = delete;

  // Enable move operations
  SortOrderBuilder(SortOrderBuilder&&) noexcept;
  SortOrderBuilder& operator=(SortOrderBuilder&&) noexcept;

 private:
  /// \brief Private constructor for building from Schema
  explicit SortOrderBuilder(const Schema* schema);

  SortOrderBuilder& AddSortField(const std::shared_ptr<Term>& term,
                                 SortDirection direction, NullOrder null_order);

  /// \brief Builds an unchecked SortOrder instance.
  Result<std::unique_ptr<SortOrder>> BuildUncheckd();

  static Status CheckCompatibility(const std::unique_ptr<SortOrder>& sort_order,
                                   const Schema* schema);

  /// Internal state members
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg
