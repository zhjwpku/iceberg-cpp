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

#include <arrow/filesystem/localfs.h>
#include <avro/GenericDatum.hh>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/avro/avro_reader.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_reader.h"
#include "iceberg/schema.h"
#include "matchers.h"
#include "temp_file_test_base.h"
#include "test_common.h"

namespace iceberg {

class ManifestListReaderV1Test : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { avro::AvroReader::Register(); }

  void SetUp() override {
    local_fs_ = std::make_shared<::arrow::fs::LocalFileSystem>();
    file_io_ = std::make_shared<iceberg::arrow::ArrowFileSystemFileIO>(local_fs_);
  }

  std::shared_ptr<::arrow::fs::LocalFileSystem> local_fs_;
  std::shared_ptr<FileIO> file_io_;

  void TestManifestListReading(const std::string& resource_name,
                               const std::vector<ManifestFile>& expected_manifest_list) {
    std::string path = GetResourcePath(resource_name);
    auto manifest_reader_result = ManifestListReader::MakeReader(path, file_io_);
    ASSERT_EQ(manifest_reader_result.has_value(), true);

    auto manifest_reader = std::move(manifest_reader_result.value());
    auto read_result = manifest_reader->Files();
    ASSERT_EQ(read_result.has_value(), true);
    ASSERT_EQ(read_result.value().size(), expected_manifest_list.size());
    ASSERT_EQ(read_result.value(), expected_manifest_list);
  }
};

TEST_F(ManifestListReaderV1Test, PartitionTest) {
  std::vector<std::string> paths = {
      "iceberg-warehouse/db/v1_partition_test/metadata/"
      "eafd2972-f58e-4185-9237-6378f564787e-m1.avro",
      "iceberg-warehouse/db/v1_partition_test/metadata/"
      "eafd2972-f58e-4185-9237-6378f564787e-m0.avro"};
  std::vector<int64_t> file_size = {6185, 6113};
  std::vector<int64_t> snapshot_id = {7532614258660258098, 7532614258660258098};

  std::vector<std::vector<std::uint8_t>> lower_bounds = {
      {0x32, 0x30, 0x32, 0x32, 0x2D, 0x30, 0x32, 0x2D, 0x32, 0x32},
      {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x32}};

  std::vector<std::vector<std::uint8_t>> upper_bounds = {
      {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x33},
      {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x33}};

  std::vector<ManifestFile> expected_manifest_list = {
      {.manifest_path = paths[0],
       .manifest_length = file_size[0],
       .partition_spec_id = 0,
       .added_snapshot_id = snapshot_id[0],
       .added_files_count = 4,
       .existing_files_count = 0,
       .deleted_files_count = 0,
       .added_rows_count = 6,
       .existing_rows_count = 0,
       .deleted_rows_count = 0,
       .partitions = {{.contains_null = false,
                       .contains_nan = false,
                       .lower_bound = lower_bounds[0],
                       .upper_bound = upper_bounds[0]}}},

      {.manifest_path = paths[1],
       .manifest_length = file_size[1],
       .partition_spec_id = 0,
       .added_snapshot_id = snapshot_id[1],
       .added_files_count = 0,
       .existing_files_count = 0,
       .deleted_files_count = 2,
       .added_rows_count = 0,
       .existing_rows_count = 0,
       .deleted_rows_count = 6,
       .partitions = {{.contains_null = false,
                       .contains_nan = false,
                       .lower_bound = lower_bounds[1],
                       .upper_bound = upper_bounds[1]}}}};

  TestManifestListReading(
      "snap-7532614258660258098-1-eafd2972-f58e-4185-9237-6378f564787e.avro",
      expected_manifest_list);
}

TEST_F(ManifestListReaderV1Test, ComplexTypeTest) {
  std::vector<std::string> paths = {
      "iceberg-warehouse/db/v1_type_test/metadata/"
      "aeffe099-3bac-4011-bc17-5875210d8dc0-m1.avro",
      "iceberg-warehouse/db/v1_type_test/metadata/"
      "aeffe099-3bac-4011-bc17-5875210d8dc0-m0.avro"};
  std::vector<int64_t> file_size = {6498, 6513};
  std::vector<int64_t> snapshot_id = {4134160420377642835, 4134160420377642835};

  std::vector<ManifestFile> expected_manifest_list = {
      {
          .manifest_path = paths[0],
          .manifest_length = file_size[0],
          .partition_spec_id = 0,
          .added_snapshot_id = snapshot_id[0],
          .added_files_count = 1,
          .existing_files_count = 0,
          .deleted_files_count = 0,
          .added_rows_count = 2,
          .existing_rows_count = 0,
          .deleted_rows_count = 0,
      },

      {.manifest_path = paths[1],
       .manifest_length = file_size[1],
       .partition_spec_id = 0,
       .added_snapshot_id = snapshot_id[1],
       .added_files_count = 0,
       .existing_files_count = 0,
       .deleted_files_count = 1,
       .added_rows_count = 0,
       .existing_rows_count = 0,
       .deleted_rows_count = 3}};

  TestManifestListReading(
      "snap-4134160420377642835-1-aeffe099-3bac-4011-bc17-5875210d8dc0.avro",
      expected_manifest_list);
}

TEST_F(ManifestListReaderV1Test, PartitionComplexTypeTest) {
  std::vector<std::string> paths = {
      "iceberg-warehouse/db2/v1_complex_partition_test/metadata/"
      "5d690750-8fb4-4cd1-8ae7-85c7b39abe14-m0.avro",
      "iceberg-warehouse/db2/v1_complex_partition_test/metadata/"
      "5d690750-8fb4-4cd1-8ae7-85c7b39abe14-m1.avro"};
  std::vector<int64_t> file_size = {6402, 6318};
  std::vector<int64_t> snapshot_id = {7522296285847100621, 7522296285847100621};

  std::vector<std::vector<std::uint8_t>> lower_bounds = {
      {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x32},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x32},
      {0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

  std::vector<std::vector<std::uint8_t>> upper_bounds = {
      {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x34},
      {0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
      {0x32, 0x30, 0x32, 0x32, 0x2D, 0x32, 0x2D, 0x32, 0x33},
      {0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};
  std::vector<ManifestFile> expected_manifest_list = {
      {.manifest_path = paths[0],
       .manifest_length = file_size[0],
       .partition_spec_id = 0,
       .added_snapshot_id = snapshot_id[0],
       .added_files_count = 0,
       .existing_files_count = 3,
       .deleted_files_count = 1,
       .added_rows_count = 0,
       .existing_rows_count = 4,
       .deleted_rows_count = 2,
       .partitions = {{.contains_null = false,
                       .contains_nan = false,
                       .lower_bound = lower_bounds[0],
                       .upper_bound = upper_bounds[0]},
                      {.contains_null = false,
                       .contains_nan = false,
                       .lower_bound = lower_bounds[1],
                       .upper_bound = upper_bounds[1]}}},

      {.manifest_path = paths[1],
       .manifest_length = file_size[1],
       .partition_spec_id = 0,
       .added_snapshot_id = snapshot_id[1],
       .added_files_count = 0,
       .existing_files_count = 1,
       .deleted_files_count = 1,
       .added_rows_count = 0,
       .existing_rows_count = 1,
       .deleted_rows_count = 1,
       .partitions = {{.contains_null = false,
                       .contains_nan = false,
                       .lower_bound = lower_bounds[2],
                       .upper_bound = upper_bounds[2]},
                      {.contains_null = false,
                       .contains_nan = false,
                       .lower_bound = lower_bounds[3],
                       .upper_bound = upper_bounds[3]}}}};
  TestManifestListReading(
      "snap-7522296285847100621-1-5d690750-8fb4-4cd1-8ae7-85c7b39abe14.avro",
      expected_manifest_list);
}

}  // namespace iceberg
