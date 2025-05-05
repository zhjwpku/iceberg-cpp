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

/// \file iceberg/manifest_list.h

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief The type of files tracked by the manifest, either data or delete files; 0 for
/// all v1 manifests
enum class ManifestContent {
  /// The manifest content is data.
  kData = 0,
  /// The manifest content is deletes.
  kDeletes = 1,
};

/// \brief Get the relative manifest content type name
ICEBERG_EXPORT constexpr std::string_view ManifestContentToString(
    ManifestContent type) noexcept {
  switch (type) {
    case ManifestContent::kData:
      return "data";
    case ManifestContent::kDeletes:
      return "deletes";
  }
}

/// \brief Get the relative manifest content type from name
ICEBERG_EXPORT constexpr Result<ManifestContent> ManifestContentFromString(
    std::string_view str) noexcept {
  if (str == "data") return ManifestContent::kData;
  if (str == "deletes") return ManifestContent::kDeletes;
  return InvalidArgument("Invalid manifest content type: {}", str);
}

struct ICEBERG_EXPORT FieldSummary {
  /// Field id: 509
  /// Whether the manifest contains at least one partition with a null value for the field
  bool contains_null;
  /// Field id: 518
  /// Whether the manifest contains at least one partition with a NaN value for the field
  std::optional<bool> contains_nan;
  /// Field id: 510
  /// Lower bound for the non-null, non-NaN values in the partition field, or null if all
  /// values are null or NaN
  std::optional<std::vector<uint8_t>> lower_bound;
  /// Field id: 511
  /// Upper bound for the non-null, non-NaN values in the partition field, or null if all
  /// values are null or NaN
  std::optional<std::vector<uint8_t>> upper_bound;

  static const SchemaField CONTAINS_NULL;
  static const SchemaField CONTAINS_NAN;
  static const SchemaField LOWER_BOUND;
  static const SchemaField UPPER_BOUND;

  static StructType GetType();
};

/// \brief Entry in a manifest list.
struct ICEBERG_EXPORT ManifestFile {
  /// Field id: 500
  /// Location of the manifest file
  std::string manifest_path;
  /// Field id: 501
  /// Length of the manifest file in bytes
  int64_t manifest_length;
  /// Field id: 502
  /// ID of a partition spec used to write the manifest; must be listed in table metadata
  /// partition-specs
  int32_t partition_spec_id;
  /// Field id: 517
  /// The type of files tracked by the manifest, either data or delete files; 0 for all v1
  /// manifests
  ManifestContent content;
  /// Field id: 515
  /// The sequence number when the manifest was added to the table; use 0 when reading v1
  /// manifest lists
  int64_t sequence_number;
  /// Field id: 516
  /// The minimum data sequence number of all live data or delete files in the manifest;
  /// use 0 when reading v1 manifest lists
  int64_t min_sequence_number;
  /// Field id: 503
  /// ID of the snapshot where the manifest file was added
  int64_t added_snapshot_id;
  /// Field id: 504
  /// Number of entries in the manifest that have status ADDED (1), when null this is
  /// assumed to be non-zero
  std::optional<int32_t> added_files_count;
  /// Field id: 505
  /// Number of entries in the manifest that have status EXISTING (0), when null this is
  /// assumed to be non-zero
  std::optional<int32_t> existing_files_count;
  /// Field id: 506
  /// Number of entries in the manifest that have status DELETED (2), when null this is
  /// assumed to be non-zero
  std::optional<int32_t> deleted_files_count;
  /// Field id: 512
  /// Number of rows in all of files in the manifest that have status ADDED, when null
  /// this is assumed to be non-zero
  std::optional<int64_t> added_rows_count;
  /// Field id: 513
  /// Number of rows in all of files in the manifest that have status EXISTING, when null
  /// this is assumed to be non-zero
  std::optional<int64_t> existing_rows_count;
  /// Field id: 514
  /// Number of rows in all of files in the manifest that have status DELETED, when null
  /// this is assumed to be non-zero
  std::optional<int64_t> deleted_rows_count;
  /// Field id: 507
  /// Element field id: 508
  /// A list of field summaries for each partition field in the spec. Each field in the
  /// list corresponds to a field in the manifest file's partition spec.
  std::vector<FieldSummary> partitions;
  /// Field id: 519
  /// Implementation-specific key metadata for encryption
  std::vector<uint8_t> key_metadata;
  /// Field id: 520
  /// The starting _row_id to assign to rows added by ADDED data files
  int64_t first_row_id;

  /// \brief Checks if this manifest file contains entries with ADDED status.
  [[nodiscard]] bool has_added_files() const {
    return added_files_count.has_value() && *added_files_count > 0;
  }

  /// \brief Checks if this manifest file contains entries with EXISTING status.
  [[nodiscard]] bool has_existing_files() const {
    return existing_files_count.has_value() && *existing_files_count > 0;
  }

  /// \brief Checks if this manifest file contains entries with DELETED status
  [[nodiscard]] bool has_deleted_files() const {
    return deleted_files_count.has_value() && *deleted_files_count > 0;
  }

  static const SchemaField MANIFEST_PATH;
  static const SchemaField MANIFEST_LENGTH;
  static const SchemaField PARTITION_SPEC_ID;
  static const SchemaField CONTENT;
  static const SchemaField SEQUENCE_NUMBER;
  static const SchemaField MIN_SEQUENCE_NUMBER;
  static const SchemaField ADDED_SNAPSHOT_ID;
  static const SchemaField ADDED_FILES_COUNT;
  static const SchemaField EXISTING_FILES_COUNT;
  static const SchemaField DELETED_FILES_COUNT;
  static const SchemaField ADDED_ROWS_COUNT;
  static const SchemaField EXISTING_ROWS_COUNT;
  static const SchemaField DELETED_ROWS_COUNT;
  static const SchemaField PARTITIONS;
  static const SchemaField KEY_METADATA;
  static const SchemaField FIRST_ROW_ID;

  static Schema schema();
};

/// Snapshots are embedded in table metadata, but the list of manifests for a snapshot are
/// stored in a separate manifest list file.
///
/// A new manifest list is written for each attempt to commit a snapshot because the list
/// of manifests always changes to produce a new snapshot. When a manifest list is
/// written, the (optimistic) sequence number of the snapshot is written for all new
/// manifest files tracked by the list.
///
/// A manifest list includes summary metadata that can be used to avoid scanning all of
/// the manifests in a snapshot when planning a table scan. This includes the number of
/// added, existing, and deleted files, and a summary of values for each field of the
/// partition spec used to write the manifest.
struct ManifestList {
  /// Entries in a manifest list.
  std::vector<ManifestFile> entries;
};

}  // namespace iceberg
