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

#include "iceberg/update/expire_snapshots.h"

#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"

namespace iceberg {

class ExpireSnapshotsTest : public UpdateTestBase {};

TEST_F(ExpireSnapshotsTest, DefaultExpireByAge) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);
  EXPECT_EQ(result.snapshot_ids_to_remove.at(0), 3051729675574597004);
}

TEST_F(ExpireSnapshotsTest, KeepAll) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->RetainLast(2);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_TRUE(result.snapshot_ids_to_remove.empty());
  EXPECT_TRUE(result.refs_to_remove.empty());
}

TEST_F(ExpireSnapshotsTest, ExpireById) {
  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
  update->ExpireSnapshotId(3051729675574597004);
  ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
  EXPECT_EQ(result.snapshot_ids_to_remove.size(), 1);
  EXPECT_EQ(result.snapshot_ids_to_remove.at(0), 3051729675574597004);
}

TEST_F(ExpireSnapshotsTest, ExpireOlderThan) {
  struct TestCase {
    int64_t expire_older_than;
    size_t expected_num_expired;
  };
  const std::vector<TestCase> test_cases = {
      {.expire_older_than = 1515100955770 - 1, .expected_num_expired = 0},
      {.expire_older_than = 1515100955770 + 1, .expected_num_expired = 1}};
  for (const auto& test_case : test_cases) {
    ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewExpireSnapshots());
    update->ExpireOlderThan(test_case.expire_older_than);
    ICEBERG_UNWRAP_OR_FAIL(auto result, update->Apply());
    EXPECT_EQ(result.snapshot_ids_to_remove.size(), test_case.expected_num_expired);
  }
}

}  // namespace iceberg
