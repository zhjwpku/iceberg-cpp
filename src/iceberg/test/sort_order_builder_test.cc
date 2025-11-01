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

#include <cstddef>
#include <memory>

#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep

namespace iceberg {

class SortOrderBuilderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    field1_ = std::make_unique<SchemaField>(1, "x", int32(), true);
    field2_ = std::make_unique<SchemaField>(2, "y", string(), true);
    field3_ = std::make_unique<SchemaField>(3, "time", timestamp(), true);

    schema_ = std::make_unique<Schema>(
        std::vector<SchemaField>{*field1_, *field2_, *field3_}, 1);

    term1_ = Expressions::Ref("x");
    term2_ = Expressions::Ref("y");
    term3_ = Expressions::Bucket("x", 10);
    term4_ = Expressions::Day("time");
  }

  std::unique_ptr<Schema> schema_;
  std::unique_ptr<SchemaField> field1_;
  std::unique_ptr<SchemaField> field2_;
  std::unique_ptr<SchemaField> field3_;

  // NamedReference
  std::shared_ptr<Term> term1_;
  std::shared_ptr<Term> term2_;

  // UnboundTransform
  std::shared_ptr<Term> term3_;
  std::shared_ptr<Term> term4_;
};

void AssertSortField(const SortField& field, int source_id, SortDirection direction,
                     const Transform& transform) {
  ASSERT_EQ(field.source_id(), source_id);
  ASSERT_EQ(field.direction(), direction);
  ASSERT_EQ(*field.transform(), transform);
}

TEST_F(SortOrderBuilderTest, Asc) {
  auto builder = SortOrderBuilder::BuildFromSchema(schema_.get());
  builder->Asc(term1_, NullOrder::kFirst).WithOrderId(1);

  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, builder->Build());
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_sorted());
  AssertSortField(sort_order->fields()[0], 1, SortDirection::kAscending,
                  *Transform::Identity());
}

TEST_F(SortOrderBuilderTest, Desc) {
  auto builder = SortOrderBuilder::BuildFromSchema(schema_.get());
  builder->Desc(term1_, NullOrder::kFirst).WithOrderId(1);

  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, builder->Build());
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_sorted());
  AssertSortField(sort_order->fields()[0], 1, SortDirection::kDescending,
                  *Transform::Identity());
}

TEST_F(SortOrderBuilderTest, SortBy) {
  auto builder = SortOrderBuilder::BuildFromSchema(schema_.get());
  builder->SortBy("y", SortDirection::kAscending, NullOrder::kFirst).WithOrderId(1);

  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, builder->Build());
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_sorted());
  AssertSortField(sort_order->fields()[0], 2, SortDirection::kAscending,
                  *Transform::Identity());
}

TEST_F(SortOrderBuilderTest, SortByTerm) {
  auto builder = SortOrderBuilder::BuildFromSchema(schema_.get());
  builder->SortBy(term2_, SortDirection::kAscending, NullOrder::kFirst).WithOrderId(1);

  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, builder->Build());
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_sorted());
  AssertSortField(sort_order->fields()[0], 2, SortDirection::kAscending,
                  *Transform::Identity());
}

TEST_F(SortOrderBuilderTest, CaseSensitive) {
  auto builder = SortOrderBuilder::BuildFromSchema(schema_.get());
  builder->CaseSensitive(false)
      .SortBy("Y", SortDirection::kAscending, NullOrder::kFirst)
      .WithOrderId(1);

  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, builder->Build());
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_sorted());
  AssertSortField(sort_order->fields()[0], 2, SortDirection::kAscending,
                  *Transform::Identity());
}

TEST_F(SortOrderBuilderTest, AddSortField) {
  auto builder = SortOrderBuilder::BuildFromSchema(schema_.get());
  builder
      ->AddSortField(3, Transform::Month(), SortDirection::kAscending, NullOrder::kFirst)
      .WithOrderId(1);

  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, builder->Build());
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_sorted());
  AssertSortField(sort_order->fields()[0], 3, SortDirection::kAscending,
                  *Transform::Month());
}

TEST_F(SortOrderBuilderTest, BucketTransform) {
  auto builder = SortOrderBuilder::BuildFromSchema(schema_.get());
  builder->SortBy(term3_, SortDirection::kAscending, NullOrder::kFirst).WithOrderId(1);

  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, builder->Build());
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_sorted());
  AssertSortField(sort_order->fields()[0], 1, SortDirection::kAscending,
                  *Transform::Bucket(10));
}

TEST_F(SortOrderBuilderTest, DayTransform) {
  auto builder = SortOrderBuilder::BuildFromSchema(schema_.get());
  builder->SortBy(term4_, SortDirection::kDescending, NullOrder::kLast).WithOrderId(1);

  ICEBERG_UNWRAP_OR_FAIL(auto sort_order, builder->Build());
  ASSERT_NE(sort_order, nullptr);

  EXPECT_TRUE(sort_order->is_sorted());
  AssertSortField(sort_order->fields()[0], 3, SortDirection::kDescending,
                  *Transform::Day());
}

}  // namespace iceberg
