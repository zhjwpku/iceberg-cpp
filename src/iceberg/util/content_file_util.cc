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

#include "iceberg/util/content_file_util.h"

#include <format>
#include <utility>

#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/util/conversions.h"
#include "iceberg/util/endian.h"
#include "iceberg/util/hour_bounds_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/math_util_internal.h"
#include "iceberg/util/temporal_util.h"

namespace iceberg {

bool ContentFileUtil::IsDV(const DataFile& file) {
  return file.file_format == FileFormatType::kPuffin;
}

Result<std::optional<std::string>> ContentFileUtil::ReferencedDataFile(
    const DataFile& file) {
  // Equality deletes don't reference a specific data file
  if (file.content == DataFile::Content::kEqualityDeletes) {
    return std::nullopt;
  }

  // If referenced_data_file is set, return it
  if (file.referenced_data_file.has_value()) {
    return file.referenced_data_file;
  }

  // Try to derive from lower/upper bounds on file_path column
  auto lower_it = file.lower_bounds.find(MetadataColumns::kDeleteFilePathColumnId);
  if (lower_it == file.lower_bounds.end() || lower_it->second.empty()) {
    return std::nullopt;
  }

  auto upper_it = file.upper_bounds.find(MetadataColumns::kDeleteFilePathColumnId);
  if (upper_it == file.upper_bounds.end() || upper_it->second.empty()) {
    return std::nullopt;
  }

  // Check if lower and upper bounds are equal
  if (lower_it->second == upper_it->second) {
    // Convert the binary bound to a string
    ICEBERG_ASSIGN_OR_RAISE(auto string_literal,
                            Conversions::FromBytes(*string(), lower_it->second));
    return std::get<std::string>(string_literal);
  }

  return std::nullopt;
}

Result<bool> ContentFileUtil::IsFileScoped(const DataFile& file) {
  ICEBERG_ASSIGN_OR_RAISE(auto referenced_data_file, ReferencedDataFile(file));
  return referenced_data_file.has_value();
}

bool ContentFileUtil::ContainsSingleDV(std::span<const std::shared_ptr<DataFile>> files) {
  return files.size() == 1 && IsDV(*files[0]);
}

std::string ContentFileUtil::DVDesc(const DataFile& file) {
  return std::format("DV{{location={}, offset={}, length={}, referencedDataFile={}}}",
                     file.file_path, file.content_offset.value_or(-1),
                     file.content_size_in_bytes.value_or(-1),
                     file.referenced_data_file.value_or(""));
}

int64_t ContentFileUtil::ContentSizeInBytes(const DataFile& file) {
  // content_size_in_bytes is only meaningful for deletion vectors; other content files
  // (data files, positional/equality delete files) must use file_size_in_bytes.
  return IsDV(file) ? file.content_size_in_bytes.value_or(file.file_size_in_bytes)
                    : file.file_size_in_bytes;
}

void ContentFileUtil::DropAllStats(DataFile& data_file) {
  data_file.column_sizes.clear();
  data_file.value_counts.clear();
  data_file.null_value_counts.clear();
  data_file.nan_value_counts.clear();
  data_file.lower_bounds.clear();
  data_file.upper_bounds.clear();
}

namespace {

template <typename V>
void DropUnselectedColumnStats(std::map<int32_t, V>& map,
                               const std::unordered_set<int32_t>& columns) {
  for (auto it = map.begin(); it != map.end();) {
    if (!columns.contains(it->first)) {
      it = map.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace

void ContentFileUtil::DropUnselectedStats(
    DataFile& data_file, const std::unordered_set<int32_t>& selected_columns) {
  DropUnselectedColumnStats<int64_t>(data_file.column_sizes, selected_columns);
  DropUnselectedColumnStats<int64_t>(data_file.value_counts, selected_columns);
  DropUnselectedColumnStats<int64_t>(data_file.null_value_counts, selected_columns);
  DropUnselectedColumnStats<int64_t>(data_file.nan_value_counts, selected_columns);
  DropUnselectedColumnStats<std::vector<uint8_t>>(data_file.lower_bounds,
                                                  selected_columns);
  DropUnselectedColumnStats<std::vector<uint8_t>>(data_file.upper_bounds,
                                                  selected_columns);
}

Result<std::optional<std::pair<int32_t, int32_t>>> internal::ReadHourBounds(
    const DataFile& file, const SchemaField& field) {
  const auto read_hour = [&](const auto& bounds) -> Result<std::optional<int32_t>> {
    auto it = bounds.find(field.field_id());
    if (it == bounds.end()) {
      return std::nullopt;
    }
    const auto& bytes = it->second;
    int64_t hour;
    if (bytes.size() == sizeof(int64_t)) {
      constexpr int64_t kMicrosPerHour = kSecondsPerHour * kMicrosPerSecond;
      hour = FloorDiv(ReadLittleEndian<int64_t>(bytes.data()), kMicrosPerHour);
    } else if (bytes.size() == sizeof(int32_t) &&
               field.type()->type_id() == TypeId::kTimestamp) {
      // A legacy date bound is already in whole days. Convert directly to hours.
      hour = ReadLittleEndian<int32_t>(bytes.data()) * kHoursPerDay;
    } else {
      return InvalidArgument("Invalid timestamp bound size: {}", bytes.size());
    }
    if (!std::in_range<int32_t>(hour)) {
      return std::nullopt;
    }
    return static_cast<int32_t>(hour);
  };
  ICEBERG_ASSIGN_OR_RAISE(auto lower, read_hour(file.lower_bounds));
  if (!lower) {
    return std::nullopt;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto upper, read_hour(file.upper_bounds));
  if (!upper) {
    return std::nullopt;
  }
  return std::pair{*lower, *upper};
}

}  // namespace iceberg
