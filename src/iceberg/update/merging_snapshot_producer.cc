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

#include "iceberg/update/merging_snapshot_producer.h"

#include <algorithm>
#include <format>
#include <map>
#include <ranges>

#include <fmt/format.h>

#include "iceberg/constants.h"
#include "iceberg/exception.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_group.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_util_internal.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/transaction.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"

namespace iceberg {

namespace {

// Operations that validate added data files
const std::unordered_set<std::string> kValidateAddedFilesOperations = {
    DataOperation::kAppend, DataOperation::kOverwrite};

// Operations that validate data files exist
const std::unordered_set<std::string> kValidateDataFilesExistOperations = {
    DataOperation::kOverwrite, DataOperation::kReplace, DataOperation::kDelete};

// Operations that skip delete when validating data files exist
const std::unordered_set<std::string> kValidateDataFilesExistSkipDeleteOperations = {
    DataOperation::kOverwrite, DataOperation::kReplace};

// Operations that validate added delete files
const std::unordered_set<std::string> kValidateAddedDeleteFilesOperations = {
    DataOperation::kOverwrite, DataOperation::kDelete};

// Operations that validate added DVs
const std::unordered_set<std::string> kValidateAddedDVsOperations = {
    DataOperation::kOverwrite, DataOperation::kDelete, DataOperation::kReplace};

// Initial sequence number for new tables
constexpr int64_t kInitialSequenceNumber = 0;

// Convert vector of partition specs to unordered_map
std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> SpecsToMap(
    const std::vector<std::shared_ptr<PartitionSpec>>& specs) {
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> result;
  for (const auto& spec : specs) {
    if (spec) {
      result[spec->spec_id()] = spec;
    }
  }
  return result;
}

// Compute a hash for a group of manifests (for cache key).
size_t HashManifestGroup(const std::vector<ManifestFile>& group) {
  size_t hash = 0;
  for (const auto& m : group) {
    hash ^= std::hash<std::string>{}(m.manifest_path) + 0x9e3779b9 + (hash << 6) +
            (hash >> 2);
  }
  return hash;
}

// Get the size of a manifest for bin packing (uses manifest_length).
int64_t ManifestSize(const ManifestFile& m) {
  return m.manifest_length > 0 ? m.manifest_length : 1;
}

// Helper to join strings
std::string JoinStrings(const std::vector<std::string>& strings,
                        std::string_view delimiter) {
  if (strings.empty()) return "";
  std::string result;
  for (size_t i = 0; i < strings.size(); ++i) {
    if (i > 0) result.append(delimiter);
    result.append(strings[i]);
  }
  return result;
}

}  // anonymous namespace

// ============================================================================
// ManifestFilterManager Implementation
// ============================================================================

class ManifestFilterManager::Impl {
 public:
  Impl(std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
       std::shared_ptr<FileIO> io, int64_t snapshot_id)
      : specs_by_id_(std::move(specs_by_id)),
        io_(std::move(io)),
        snapshot_id_(snapshot_id) {}

  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id_;
  std::shared_ptr<FileIO> io_;
  int64_t snapshot_id_;
  bool case_sensitive_ = true;
  bool fail_any_delete_ = false;
  bool fail_missing_delete_paths_ = false;
  std::unordered_set<std::string> delete_paths_;
  std::shared_ptr<Expression> delete_expression_;
  std::unordered_set<PartitionKey, PartitionKeyHash, PartitionKeyEqual> drop_partitions_;
  std::vector<ManifestFile> rewritten_manifests_;
  SnapshotSummaryBuilder summary_builder_;
  int64_t min_sequence_number_ = 0;
  std::unordered_set<std::string> removed_data_file_paths_;

  bool ContainsDeletes() const {
    return !delete_paths_.empty() || delete_expression_ != nullptr ||
           !drop_partitions_.empty();
  }

  Result<std::vector<ManifestEntry>> FilterEntries(const std::shared_ptr<Schema>& schema,
                                                   const ManifestFile& manifest) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto reader, ManifestReader::Make(manifest, io_, schema,
                                          specs_by_id_[manifest.partition_spec_id]));

    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());

    std::vector<ManifestEntry> filtered;
    filtered.reserve(entries.size());

    const bool is_delete_manifest = manifest.content == ManifestContent::kDeletes;

    for (auto& entry : entries) {
      // Check if entry should be kept
      bool keep = true;

      if (entry.data_file == nullptr) {
        continue;
      }

      // For delete manifests: drop delete files older than min_sequence_number
      if (is_delete_manifest && entry.sequence_number.has_value() &&
          entry.sequence_number.value() < min_sequence_number_) {
        keep = false;
      }

      // For delete manifests: remove orphaned DVs that reference deleted data files
      if (keep && is_delete_manifest && !removed_data_file_paths_.empty() &&
          ContentFileUtil::IsDV(*entry.data_file)) {
        auto ref_result = ContentFileUtil::ReferencedDataFile(*entry.data_file);
        if (ref_result.has_value() && ref_result.value().has_value() &&
            removed_data_file_paths_.contains(ref_result.value().value())) {
          keep = false;
        }
      }

      // Check delete paths
      if (keep && delete_paths_.contains(entry.data_file->file_path)) {
        if (fail_any_delete_) {
          return ValidationFailed("Detected deleted file in delete paths: {}",
                                  entry.data_file->file_path);
        }
        keep = false;
      }

      // Check delete expression
      if (keep && delete_expression_ != nullptr) {
        // TODO: Evaluate expression against file data
        // For now, simplified check based on partition
      }

      // Check drop partitions
      if (keep && !drop_partitions_.empty()) {
        PartitionKeyRef key(entry.data_file->partition_spec_id.value(),
                            entry.data_file->partition);
        if (drop_partitions_.contains(key)) {
          if (fail_any_delete_) {
            return ValidationFailed("Detected deleted file in dropped partition");
          }
          keep = false;
        }
      }

      if (keep) {
        filtered.push_back(std::move(entry));
      } else {
        // Track deletion in summary
        ICEBERG_RETURN_UNEXPECTED(summary_builder_.DeletedFile(
            *specs_by_id_[manifest.partition_spec_id], *entry.data_file));
      }
    }

    return filtered;
  }
};

ManifestFilterManager::ManifestFilterManager(
    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
    std::shared_ptr<FileIO> io, int64_t snapshot_id)
    : impl_(std::make_unique<Impl>(std::move(specs_by_id), std::move(io), snapshot_id)) {}

ManifestFilterManager::~ManifestFilterManager() = default;

ManifestFilterManager::ManifestFilterManager(ManifestFilterManager&&) noexcept = default;
ManifestFilterManager& ManifestFilterManager::operator=(
    ManifestFilterManager&&) noexcept = default;

ManifestFilterManager& ManifestFilterManager::CaseSensitive(bool case_sensitive) {
  impl_->case_sensitive_ = case_sensitive;
  return *this;
}

void ManifestFilterManager::Delete(const std::string& path) {
  impl_->delete_paths_.insert(path);
}

void ManifestFilterManager::Delete(const DataFile& file) {
  impl_->delete_paths_.insert(file.file_path);
}

void ManifestFilterManager::DeleteByRowFilter(std::shared_ptr<Expression> expr) {
  if (impl_->delete_expression_ == nullptr) {
    impl_->delete_expression_ = std::move(expr);
  } else {
    // Combine with existing expression using OR (match Java: Expressions.or)
    auto result = Or::MakeFolded(impl_->delete_expression_, std::move(expr));
    if (result.has_value()) {
      impl_->delete_expression_ = std::move(result.value());
    }
  }
}

void ManifestFilterManager::DropPartition(int32_t spec_id,
                                          const PartitionValues& partition) {
  impl_->drop_partitions_.emplace(spec_id, partition);
}

void ManifestFilterManager::FailAnyDelete() { impl_->fail_any_delete_ = true; }

void ManifestFilterManager::FailMissingDeletePaths() {
  impl_->fail_missing_delete_paths_ = true;
}

void ManifestFilterManager::DropDeleteFilesOlderThan(int64_t sequence_number) {
  if (sequence_number < 0) [[unlikely]] {
    throw IcebergError(
        std::format("Invalid minimum data sequence number: {}", sequence_number));
  }
  impl_->min_sequence_number_ = sequence_number;
}

void ManifestFilterManager::RemoveDanglingDeletesFor(
    const std::unordered_set<std::string>& data_file_paths) {
  impl_->removed_data_file_paths_ = data_file_paths;
}

bool ManifestFilterManager::ContainsDeletes() const { return impl_->ContainsDeletes(); }

const std::unordered_set<std::string>& ManifestFilterManager::FilesToDelete() const {
  return impl_->delete_paths_;
}

Result<std::vector<ManifestFile>> ManifestFilterManager::FilterManifests(
    const std::shared_ptr<Schema>& schema, std::span<const ManifestFile> manifests) {
  std::vector<ManifestFile> filtered_manifests;
  filtered_manifests.reserve(manifests.size());

  for (const auto& manifest : manifests) {
    ICEBERG_ASSIGN_OR_RAISE(auto filtered_entries,
                            impl_->FilterEntries(schema, manifest));

    if (filtered_entries.empty()) {
      // All entries were deleted, skip this manifest
      continue;
    }

    if (filtered_entries.size() ==
        static_cast<size_t>(manifest.added_files_count.value_or(0) +
                            manifest.existing_files_count.value_or(0) +
                            manifest.deleted_files_count.value_or(0))) {
      // No entries were filtered, keep original manifest
      filtered_manifests.push_back(manifest);
      continue;
    }

    // Manifest was modified, need to rewrite it
    // For now, create a new manifest with filtered entries
    // TODO: Implement manifest rewriting
    filtered_manifests.push_back(manifest);
  }

  return filtered_manifests;
}

SnapshotSummaryBuilder ManifestFilterManager::BuildSummary(
    std::span<const ManifestFile> /*filtered*/) {
  return impl_->summary_builder_;
}

void ManifestFilterManager::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  for (const auto& manifest : impl_->rewritten_manifests_) {
    if (!committed.contains(manifest.manifest_path)) {
      std::ignore = impl_->io_->DeleteFile(manifest.manifest_path);
    }
  }
  impl_->rewritten_manifests_.clear();
}

// ============================================================================
// ManifestMergeManager
// ============================================================================

ManifestMergeManager::ManifestMergeManager(int64_t target_size_bytes,
                                           int32_t min_count_to_merge, bool merge_enabled,
                                           ManifestContent content)
    : target_size_bytes_(target_size_bytes),
      min_count_to_merge_(min_count_to_merge),
      merge_enabled_(merge_enabled),
      content_(content) {}

std::vector<std::vector<ManifestFile>> ManifestMergeManager::PackManifests(
    const std::vector<ManifestFile>& manifests) const {
  if (manifests.empty()) {
    return {};
  }

  // Pack from the end (reverse order) so the under-filled bin is the first one.
  std::vector<std::vector<ManifestFile>> bins;
  std::vector<ManifestFile> current_bin;
  int64_t current_size = 0;

  for (auto it = manifests.rbegin(); it != manifests.rend(); ++it) {
    int64_t size = ManifestSize(*it);
    if (current_bin.empty()) {
      current_bin.push_back(*it);
      current_size = size;
    } else if (current_size + size <= target_size_bytes_) {
      current_bin.push_back(*it);
      current_size += size;
    } else {
      bins.push_back(std::move(current_bin));
      current_bin = {*it};
      current_size = size;
    }
  }
  if (!current_bin.empty()) {
    bins.push_back(std::move(current_bin));
  }

  std::reverse(bins.begin(), bins.end());
  return bins;
}

Result<ManifestFile> ManifestMergeManager::MergeManifestGroup(
    const std::vector<ManifestFile>& group) {
  if (group.empty()) {
    return InvalidArgument("Cannot merge empty manifest group");
  }
  if (group.size() == 1) {
    return group[0];
  }

  size_t cache_key = HashManifestGroup(group);
  auto it = merged_manifests_cache_.find(cache_key);
  if (it != merged_manifests_cache_.end()) {
    return it->second.second;
  }

  int32_t spec_id = group[0].partition_spec_id;
  ICEBERG_ASSIGN_OR_RAISE(auto spec, Spec(spec_id));
  ICEBERG_ASSIGN_OR_RAISE(auto writer, NewManifestWriter(spec));

  int64_t snapshot_id = SnapshotId();

  for (const auto& manifest : group) {
    ICEBERG_ASSIGN_OR_RAISE(auto reader, NewManifestReader(manifest));
    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());

    for (auto& entry : entries) {
      if (entry.data_file == nullptr) {
        continue;
      }

      if (entry.status == ManifestStatus::kDeleted) {
        if (entry.snapshot_id == snapshot_id) {
          ICEBERG_RETURN_UNEXPECTED(writer->WriteDeletedEntry(entry));
        }
      } else if (entry.status == ManifestStatus::kAdded &&
                 entry.snapshot_id == snapshot_id) {
        ICEBERG_RETURN_UNEXPECTED(
            writer->WriteAddedEntry(entry.data_file, entry.sequence_number));
      } else {
        ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
      }
    }
  }

  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_file, writer->ToManifestFile());

  merged_manifests_cache_[cache_key] = {group, manifest_file};
  for (const auto& m : group) {
    if (SnapshotId() != m.added_snapshot_id) {
      replaced_manifests_count_++;
    }
  }
  return manifest_file;
}

Result<std::vector<ManifestFile>> ManifestMergeManager::MergeManifests(
    const std::vector<ManifestFile>& manifests,
    std::optional<std::string_view> first_manifest_path) {
  if (!merge_enabled_ || manifests.empty()) {
    return std::vector<ManifestFile>(manifests);
  }

  std::map<int32_t, std::vector<ManifestFile>, std::greater<>> groups;
  for (const auto& m : manifests) {
    groups[m.partition_spec_id].push_back(m);
  }

  std::vector<ManifestFile> result;
  for (auto& [_, group] : groups) {
    std::vector<std::vector<ManifestFile>> bins = PackManifests(group);

    for (auto& bin : bins) {
      if (bin.size() == 1) {
        result.push_back(std::move(bin[0]));
      } else if (first_manifest_path.has_value() &&
                 std::any_of(bin.begin(), bin.end(),
                             [&](const ManifestFile& m) {
                               return m.manifest_path == *first_manifest_path;
                             }) &&
                 static_cast<int32_t>(bin.size()) < min_count_to_merge_) {
        for (auto& m : bin) {
          result.push_back(std::move(m));
        }
      } else {
        ICEBERG_ASSIGN_OR_RAISE(auto merged, MergeManifestGroup(bin));
        result.push_back(std::move(merged));
      }
    }
  }

  return result;
}

void ManifestMergeManager::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  std::vector<size_t> to_remove;
  int64_t snapshot_id = SnapshotId();
  for (const auto& [cache_key, cached] : merged_manifests_cache_) {
    const ManifestFile& merged = cached.second;
    if (!committed.contains(merged.manifest_path)) {
      std::ignore = DeleteFile(merged.manifest_path);
      for (const auto& m : cached.first) {
        if (snapshot_id != m.added_snapshot_id) {
          replaced_manifests_count_--;
        }
      }
      to_remove.push_back(cache_key);
    }
  }
  for (size_t key : to_remove) {
    merged_manifests_cache_.erase(key);
  }
}

// ============================================================================
// MergingSnapshotProducer::DataMergeManager and DeleteMergeManager
// ============================================================================

MergingSnapshotProducer::DataMergeManager::DataMergeManager(
    int64_t target_size_bytes, int32_t min_count_to_merge, bool merge_enabled,
    MergingSnapshotProducer* parent)
    : ManifestMergeManager(target_size_bytes, min_count_to_merge, merge_enabled,
                           ManifestContent::kData),
      parent_(parent) {}

int64_t MergingSnapshotProducer::DataMergeManager::SnapshotId() {
  return parent_->SnapshotId();
}

Result<std::shared_ptr<PartitionSpec>> MergingSnapshotProducer::DataMergeManager::Spec(
    int32_t spec_id) {
  return parent_->Spec(spec_id);
}

Status MergingSnapshotProducer::DataMergeManager::DeleteFile(const std::string& path) {
  return parent_->DeleteFile(path);
}

Result<std::unique_ptr<ManifestWriter>>
MergingSnapshotProducer::DataMergeManager::NewManifestWriter(
    const std::shared_ptr<PartitionSpec>& spec) {
  return parent_->NewManifestWriter(spec);
}

Result<std::unique_ptr<ManifestReader>>
MergingSnapshotProducer::DataMergeManager::NewManifestReader(
    const ManifestFile& manifest) {
  return parent_->NewManifestReader(manifest);
}

MergingSnapshotProducer::DeleteMergeManager::DeleteMergeManager(
    int64_t target_size_bytes, int32_t min_count_to_merge, bool merge_enabled,
    MergingSnapshotProducer* parent)
    : ManifestMergeManager(target_size_bytes, min_count_to_merge, merge_enabled,
                           ManifestContent::kDeletes),
      parent_(parent) {}

int64_t MergingSnapshotProducer::DeleteMergeManager::SnapshotId() {
  return parent_->SnapshotId();
}

Result<std::shared_ptr<PartitionSpec>> MergingSnapshotProducer::DeleteMergeManager::Spec(
    int32_t spec_id) {
  return parent_->Spec(spec_id);
}

Status MergingSnapshotProducer::DeleteMergeManager::DeleteFile(const std::string& path) {
  return parent_->DeleteFile(path);
}

Result<std::unique_ptr<ManifestWriter>>
MergingSnapshotProducer::DeleteMergeManager::NewManifestWriter(
    const std::shared_ptr<PartitionSpec>& spec) {
  return parent_->NewDeleteManifestWriter(spec);
}

Result<std::unique_ptr<ManifestReader>>
MergingSnapshotProducer::DeleteMergeManager::NewManifestReader(
    const ManifestFile& manifest) {
  return parent_->NewDeleteManifestReader(manifest);
}

// ============================================================================
// MergingSnapshotProducer::Impl
// ============================================================================

class MergingSnapshotProducer::Impl {
 public:
  Impl(std::string table_name, std::shared_ptr<Transaction> transaction,
       MergingSnapshotProducer* parent)
      : table_name_(std::move(table_name)),
        transaction_(std::move(transaction)),
        parent_(parent) {
    const auto& metadata = transaction_->table()->metadata();
    target_size_bytes_ =
        metadata->properties.Get(TableProperties::kManifestTargetSizeBytes);
    min_count_to_merge_ =
        metadata->properties.Get(TableProperties::kManifestMinMergeCount);
    merge_enabled_ = metadata->properties.Get(TableProperties::kManifestMergeEnabled);

    InitializeManifestManagers();
  }

  void InitializeManifestManagers() {
    data_merge_manager_ = std::make_unique<MergingSnapshotProducer::DataMergeManager>(
        target_size_bytes_, min_count_to_merge_, merge_enabled_, parent_);

    delete_merge_manager_ = std::make_unique<MergingSnapshotProducer::DeleteMergeManager>(
        target_size_bytes_, min_count_to_merge_, merge_enabled_, parent_);

    // Initialize filter managers
    const auto& metadata = transaction_->table()->metadata();
    data_filter_manager_ = std::make_unique<ManifestFilterManager>(
        SpecsToMap(metadata->partition_specs), transaction_->table()->io(),
        parent_->SnapshotId());

    delete_filter_manager_ = std::make_unique<ManifestFilterManager>(
        SpecsToMap(metadata->partition_specs), transaction_->table()->io(),
        parent_->SnapshotId());
  }

  std::string table_name_;
  std::shared_ptr<Transaction> transaction_;
  MergingSnapshotProducer* parent_;

  // Manifest managers (DataMergeManager and DeleteMergeManager extend
  // ManifestMergeManager)
  std::unique_ptr<ManifestMergeManager> data_merge_manager_;
  std::unique_ptr<ManifestMergeManager> delete_merge_manager_;
  std::unique_ptr<ManifestFilterManager> data_filter_manager_;
  std::unique_ptr<ManifestFilterManager> delete_filter_manager_;

  // Configuration
  int64_t target_size_bytes_;
  int32_t min_count_to_merge_;
  bool merge_enabled_;
  bool case_sensitive_ = true;

  // Data tracking
  std::unordered_map<int32_t, DataFileSet> new_data_files_by_spec_;
  std::optional<int64_t> new_data_files_data_sequence_number_;
  // Delete files with optional per-file data sequence number (path -> seq for dedup)
  std::unordered_map<
      int32_t, std::vector<std::pair<std::shared_ptr<DataFile>, std::optional<int64_t>>>>
      new_delete_files_by_spec_;
  std::unordered_set<std::string> new_dv_refs_;
  std::vector<ManifestFile> append_manifests_;
  std::vector<ManifestFile> rewritten_append_manifests_;
  SnapshotSummaryBuilder added_files_summary_;
  SnapshotSummaryBuilder appended_manifests_summary_;
  std::shared_ptr<Expression> delete_expression_;

  // Cached manifests
  std::vector<ManifestFile> cached_new_data_manifests_;
  bool has_new_data_files_ = false;
  std::vector<ManifestFile> cached_new_delete_manifests_;
  bool has_new_delete_files_ = false;
};

// ============================================================================
// MergingSnapshotProducer Implementation
// ============================================================================

MergingSnapshotProducer::MergingSnapshotProducer(std::string table_name,
                                                 std::shared_ptr<Transaction> transaction)
    : SnapshotUpdate(std::move(transaction)),
      impl_(std::make_unique<Impl>(std::move(table_name), transaction_, this)) {}

MergingSnapshotProducer::~MergingSnapshotProducer() = default;

MergingSnapshotProducer& MergingSnapshotProducer::CaseSensitive(bool case_sensitive) {
  impl_->case_sensitive_ = case_sensitive;
  impl_->data_filter_manager_->CaseSensitive(case_sensitive);
  impl_->delete_filter_manager_->CaseSensitive(case_sensitive);
  return *this;
}

MergingSnapshotProducer& MergingSnapshotProducer::Delete(const std::string& path) {
  impl_->data_filter_manager_->Delete(path);
  return *this;
}

MergingSnapshotProducer& MergingSnapshotProducer::Delete(const DataFile& file) {
  impl_->data_filter_manager_->Delete(file);
  return *this;
}

MergingSnapshotProducer& MergingSnapshotProducer::DeleteDeleteFile(const DataFile& file) {
  impl_->delete_filter_manager_->Delete(file);
  return *this;
}

MergingSnapshotProducer& MergingSnapshotProducer::DeleteByRowFilter(
    std::shared_ptr<Expression> expr) {
  impl_->delete_expression_ = expr;
  impl_->data_filter_manager_->DeleteByRowFilter(expr);
  impl_->delete_filter_manager_->DeleteByRowFilter(expr);
  return *this;
}

MergingSnapshotProducer& MergingSnapshotProducer::DropPartition(
    int32_t spec_id, const PartitionValues& partition) {
  impl_->data_filter_manager_->DropPartition(spec_id, partition);
  impl_->delete_filter_manager_->DropPartition(spec_id, partition);
  return *this;
}

MergingSnapshotProducer& MergingSnapshotProducer::Add(
    const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(file->partition_spec_id.has_value(),
                        "Data file must have partition spec ID");

  int32_t spec_id = file->partition_spec_id.value();
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto spec, Spec(spec_id));

  // Suppress first_row_id for new data files (match Java Delegates.suppressFirstRowId)
  std::shared_ptr<DataFile> file_to_add = file;
  if (file->first_row_id.has_value()) {
    file_to_add = std::make_shared<DataFile>(*file);
    file_to_add->first_row_id = std::nullopt;
  }

  auto& data_files = impl_->new_data_files_by_spec_[spec_id];
  auto [iter, inserted] = data_files.insert(file_to_add);
  if (inserted) {
    impl_->has_new_data_files_ = true;
    ICEBERG_BUILDER_RETURN_IF_ERROR(
        impl_->added_files_summary_.AddedFile(*spec, *file_to_add));
  }

  return *this;
}

MergingSnapshotProducer& MergingSnapshotProducer::AddDelete(
    const std::shared_ptr<DataFile>& file) {
  return AddDeleteInternal(file, std::nullopt);
}

MergingSnapshotProducer& MergingSnapshotProducer::AddDelete(
    const std::shared_ptr<DataFile>& file, int64_t data_sequence_number) {
  return AddDeleteInternal(file, std::make_optional(data_sequence_number));
}

MergingSnapshotProducer& MergingSnapshotProducer::AddDeleteInternal(
    const std::shared_ptr<DataFile>& file, std::optional<int64_t> data_sequence_number) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid delete file: null");
  ICEBERG_BUILDER_RETURN_IF_ERROR(ValidateNewDeleteFile(*file));

  int32_t spec_id = file->partition_spec_id.value();
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto spec, Spec(spec_id));

  auto& delete_files = impl_->new_delete_files_by_spec_[spec_id];
  // Check for duplicate by path
  auto it = std::find_if(
      delete_files.begin(), delete_files.end(),
      [&file](const auto& p) { return p.first->file_path == file->file_path; });
  if (it != delete_files.end()) {
    it->second = data_sequence_number;
    return *this;
  }

  delete_files.emplace_back(file, data_sequence_number);
  impl_->has_new_delete_files_ = true;
  ICEBERG_BUILDER_RETURN_IF_ERROR(impl_->added_files_summary_.AddedFile(*spec, *file));
  if (ContentFileUtil::IsDV(*file)) {
    auto ref_result = ContentFileUtil::ReferencedDataFile(*file);
    if (ref_result.has_value()) {
      if (auto ref = ref_result.value()) {
        impl_->new_dv_refs_.insert(*ref);
      }
    }
  }

  return *this;
}

MergingSnapshotProducer& MergingSnapshotProducer::AddManifest(
    const ManifestFile& manifest) {
  ICEBERG_BUILDER_CHECK(manifest.content == ManifestContent::kData,
                        "Cannot append delete manifest: {}", manifest.manifest_path);

  if (can_inherit_snapshot_id() && manifest.added_snapshot_id == kInvalidSnapshotId) {
    ICEBERG_BUILDER_CHECK(manifest.first_row_id == std::nullopt,
                          "Cannot append manifest with assigned first_row_id: {}",
                          manifest.manifest_path);
    impl_->appended_manifests_summary_.AddedManifest(manifest);
    impl_->append_manifests_.push_back(manifest);
  } else {
    // The manifest must be rewritten with this update's snapshot ID
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto copied_manifest, CopyManifest(manifest));
    impl_->rewritten_append_manifests_.push_back(std::move(copied_manifest));
  }

  return *this;
}

bool MergingSnapshotProducer::DeletesDataFiles() const {
  return impl_->data_filter_manager_->ContainsDeletes();
}

bool MergingSnapshotProducer::DeletesDeleteFiles() const {
  return impl_->delete_filter_manager_->ContainsDeletes();
}

bool MergingSnapshotProducer::AddsDataFiles() const {
  return !impl_->new_data_files_by_spec_.empty();
}

bool MergingSnapshotProducer::AddsDeleteFiles() const {
  return !impl_->new_delete_files_by_spec_.empty();
}

std::shared_ptr<Expression> MergingSnapshotProducer::RowFilter() const {
  return impl_->delete_expression_;
}

void MergingSnapshotProducer::SetNewDataFilesDataSequenceNumber(int64_t sequence_number) {
  impl_->new_data_files_data_sequence_number_ = sequence_number;
}

std::vector<std::shared_ptr<DataFile>> MergingSnapshotProducer::AddedDataFiles() const {
  std::vector<std::shared_ptr<DataFile>> result;
  for (const auto& [spec_id, files] : impl_->new_data_files_by_spec_) {
    for (const auto& file : files) {
      result.push_back(file);
    }
  }
  return result;
}

void MergingSnapshotProducer::FailAnyDelete() {
  impl_->data_filter_manager_->FailAnyDelete();
  impl_->delete_filter_manager_->FailAnyDelete();
}

void MergingSnapshotProducer::FailMissingDeletePaths() {
  impl_->data_filter_manager_->FailMissingDeletePaths();
  impl_->delete_filter_manager_->FailMissingDeletePaths();
}

Status MergingSnapshotProducer::ValidateNewDeleteFile(const DataFile& file) {
  int format_version = transaction_->table()->metadata()->format_version;

  switch (format_version) {
    case 1:
      return NotSupported("Deletes are supported in V2 and above");
    case 2:
      if (file.content != DataFile::Content::kEqualityDeletes &&
          ContentFileUtil::IsDV(file)) {
        return InvalidArgument("Must not use DVs for position deletes in V2: {}",
                               ContentFileUtil::DVDesc(file));
      }
      break;
    case 3:
    case 4:
      if (file.content != DataFile::Content::kEqualityDeletes &&
          !ContentFileUtil::IsDV(file)) {
        return InvalidArgument("Must use DVs for position deletes in V{}: {}",
                               format_version, file.file_path);
      }
      break;
    default:
      return NotSupported("Unsupported format version: {}", format_version);
  }

  return {};
}

Result<std::shared_ptr<PartitionSpec>> MergingSnapshotProducer::Spec(int32_t spec_id) {
  return transaction_->table()->metadata()->PartitionSpecById(spec_id);
}

Result<std::unique_ptr<ManifestWriter>> MergingSnapshotProducer::NewManifestWriter(
    const std::shared_ptr<PartitionSpec>& spec) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  return ManifestWriter::MakeWriter(base().format_version, SnapshotId(), ManifestPath(),
                                    transaction_->table()->io(), spec, std::move(schema),
                                    ManifestContent::kData, base().next_row_id);
}

Result<std::unique_ptr<ManifestWriter>> MergingSnapshotProducer::NewDeleteManifestWriter(
    const std::shared_ptr<PartitionSpec>& spec) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  return ManifestWriter::MakeWriter(base().format_version, SnapshotId(), ManifestPath(),
                                    transaction_->table()->io(), spec, std::move(schema),
                                    ManifestContent::kDeletes);
}

Result<std::unique_ptr<ManifestReader>> MergingSnapshotProducer::NewManifestReader(
    const ManifestFile& manifest) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec, transaction_->table()->metadata()->PartitionSpecById(
                                         manifest.partition_spec_id));
  return ManifestReader::Make(manifest, transaction_->table()->io(), std::move(schema),
                              std::move(spec));
}

Result<std::unique_ptr<ManifestReader>> MergingSnapshotProducer::NewDeleteManifestReader(
    const ManifestFile& manifest) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, base().Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec, transaction_->table()->metadata()->PartitionSpecById(
                                         manifest.partition_spec_id));
  return ManifestReader::Make(manifest, transaction_->table()->io(), std::move(schema),
                              std::move(spec));
}

Result<ManifestFile> MergingSnapshotProducer::CopyManifest(const ManifestFile& manifest) {
  const auto& current = transaction_->table()->metadata();
  ICEBERG_ASSIGN_OR_RAISE(auto schema, current->Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          current->PartitionSpecById(manifest.partition_spec_id));

  std::string new_manifest_path = ManifestPath();
  int64_t snapshot_id = SnapshotId();

  return CopyAppendManifest(manifest, transaction_->table()->io(), schema, spec,
                            snapshot_id, new_manifest_path, current->format_version,
                            &impl_->appended_manifests_summary_);
}

// ============================================================================
// Validation Methods
// ============================================================================

Result<MergingSnapshotProducer::ValidationHistory>
MergingSnapshotProducer::GetValidationHistory(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    const std::unordered_set<std::string>& operations, ManifestContent content,
    const std::shared_ptr<Snapshot>& parent) {
  ValidationHistory result;

  if (parent == nullptr) {
    return result;
  }

  // Get ancestor snapshots between starting and parent
  auto lookup = [&base](int64_t id) -> Result<std::shared_ptr<Snapshot>> {
    auto snap = base.SnapshotById(id);
    if (snap.has_value()) {
      return *snap;
    }
    return NotFound("Snapshot not found: {}", id);
  };

  // Get ancestors between parent and starting snapshot
  // Note: The C++ API may differ from Java; using AncestorsBetween instead
  ICEBERG_ASSIGN_OR_RAISE(auto ancestors, SnapshotUtil::AncestorsBetween(
                                              *transaction_->table()->metadata(),
                                              parent->snapshot_id, starting_snapshot_id));

  std::optional<std::shared_ptr<Snapshot>> last_snapshot;
  for (const auto& current_snapshot : ancestors) {
    last_snapshot = current_snapshot;

    std::string op(current_snapshot->Operation().value_or(""));
    if (operations.contains(op)) {
      result.snapshot_ids.insert(current_snapshot->snapshot_id);

      // Load manifests based on content type
      auto cached = SnapshotCache(current_snapshot.get());
      if (content == ManifestContent::kData) {
        ICEBERG_ASSIGN_OR_RAISE(auto manifests,
                                cached.DataManifests(transaction_->table()->io()));
        for (const auto& manifest : manifests) {
          if (manifest.added_snapshot_id == current_snapshot->snapshot_id) {
            result.manifests.push_back(manifest);
          }
        }
      } else {
        ICEBERG_ASSIGN_OR_RAISE(auto manifests,
                                cached.DeleteManifests(transaction_->table()->io()));
        for (const auto& manifest : manifests) {
          if (manifest.added_snapshot_id == current_snapshot->snapshot_id) {
            result.manifests.push_back(manifest);
          }
        }
      }
    }
  }

  // Validate history is continuous
  if (last_snapshot.has_value()) {
    auto last_parent = last_snapshot.value()->parent_snapshot_id;
    if (last_parent != starting_snapshot_id) {
      return ValidationFailed(
          "Cannot determine history between starting snapshot {} and the last known "
          "ancestor {}",
          starting_snapshot_id.value_or(-1), last_snapshot.value()->snapshot_id);
    }
  }

  return result;
}

int64_t MergingSnapshotProducer::StartingSequenceNumber(
    const TableMetadata& metadata, std::optional<int64_t> starting_snapshot_id) {
  if (starting_snapshot_id.has_value()) {
    auto snap = metadata.SnapshotById(starting_snapshot_id.value());
    if (snap.has_value()) {
      return snap.value()->sequence_number;
    }
  }
  return kInitialSequenceNumber;
}

Result<std::unique_ptr<DeleteFileIndex>> MergingSnapshotProducer::BuildDeleteFileIndex(
    const std::vector<ManifestFile>& delete_manifests, int64_t starting_sequence_number,
    std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set) {
  const auto& metadata = transaction_->table()->metadata();
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata->Schema());

  ICEBERG_ASSIGN_OR_RAISE(
      auto builder, DeleteFileIndex::BuilderFor(transaction_->table()->io(), schema,
                                                SpecsToMap(metadata->partition_specs),
                                                delete_manifests));

  builder.AfterSequenceNumber(starting_sequence_number);
  builder.CaseSensitive(impl_->case_sensitive_);

  if (data_filter != nullptr) {
    builder.DataFilter(data_filter);
  }

  if (partition_set != nullptr) {
    builder.FilterPartitions(std::make_shared<PartitionSet>(*partition_set));
  }

  return builder.Build();
}

Result<std::unique_ptr<DeleteFileIndex>> MergingSnapshotProducer::AddedDeleteFiles(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set,
    const std::shared_ptr<Snapshot>& parent) {
  if (parent == nullptr || base.format_version < 2) {
    // Return empty index
    ICEBERG_ASSIGN_OR_RAISE(auto schema, base.Schema());
    return DeleteFileIndex::BuilderFor(transaction_->table()->io(), schema,
                                       SpecsToMap(base.partition_specs), {})
        .value()
        .Build();
  }

  ICEBERG_ASSIGN_OR_RAISE(auto history,
                          GetValidationHistory(base, starting_snapshot_id,
                                               kValidateAddedDeleteFilesOperations,
                                               ManifestContent::kDeletes, parent));

  int64_t starting_seq = StartingSequenceNumber(base, starting_snapshot_id);
  return BuildDeleteFileIndex(history.manifests, starting_seq, data_filter,
                              partition_set);
}

Result<std::vector<ManifestEntry>> MergingSnapshotProducer::AddedDataFiles(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set,
    const std::shared_ptr<Snapshot>& parent) {
  if (parent == nullptr) {
    return std::vector<ManifestEntry>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history,
      GetValidationHistory(base, starting_snapshot_id, kValidateAddedFilesOperations,
                           ManifestContent::kData, parent));

  if (history.manifests.empty()) {
    return std::vector<ManifestEntry>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, base.Schema());
  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(transaction_->table()->io(), schema,
                          SpecsToMap(base.partition_specs), history.manifests));

  manifest_group->CaseSensitive(impl_->case_sensitive_);
  manifest_group->FilterManifestEntries([&history](const ManifestEntry& entry) {
    if (entry.snapshot_id.has_value()) {
      return history.snapshot_ids.contains(entry.snapshot_id.value());
    }
    return false;
  });
  manifest_group->IgnoreDeleted();
  manifest_group->IgnoreExisting();

  if (data_filter != nullptr) {
    manifest_group->FilterData(data_filter);
  }

  if (partition_set != nullptr) {
    manifest_group->FilterManifestEntries([partition_set](const ManifestEntry& entry) {
      if (entry.data_file == nullptr || !entry.data_file->partition_spec_id.has_value()) {
        return false;
      }
      return partition_set->contains(entry.data_file->partition_spec_id.value(),
                                     entry.data_file->partition);
    });
  }

  return manifest_group->Entries();
}

Result<std::vector<ManifestEntry>> MergingSnapshotProducer::DeletedDataFiles(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set,
    const std::shared_ptr<Snapshot>& parent) {
  if (parent == nullptr) {
    return std::vector<ManifestEntry>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history,
      GetValidationHistory(base, starting_snapshot_id, kValidateDataFilesExistOperations,
                           ManifestContent::kData, parent));

  if (history.manifests.empty()) {
    return std::vector<ManifestEntry>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, base.Schema());
  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(transaction_->table()->io(), schema,
                          SpecsToMap(base.partition_specs), history.manifests));

  manifest_group->CaseSensitive(impl_->case_sensitive_);
  manifest_group->FilterManifestEntries([&history](const ManifestEntry& entry) {
    if (entry.snapshot_id.has_value()) {
      return history.snapshot_ids.contains(entry.snapshot_id.value());
    }
    return false;
  });
  manifest_group->FilterManifestEntries([](const ManifestEntry& entry) {
    return entry.status == ManifestStatus::kDeleted;
  });
  manifest_group->IgnoreExisting();

  if (data_filter != nullptr) {
    manifest_group->FilterData(data_filter);
  }

  if (partition_set != nullptr) {
    manifest_group->FilterManifestEntries([partition_set](const ManifestEntry& entry) {
      if (entry.data_file == nullptr || !entry.data_file->partition_spec_id.has_value()) {
        return false;
      }
      return partition_set->contains(entry.data_file->partition_spec_id.value(),
                                     entry.data_file->partition);
    });
  }

  return manifest_group->Entries();
}

// Validation method implementations
Status MergingSnapshotProducer::ValidateAddedDataFiles(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_entries,
      AddedDataFiles(base, starting_snapshot_id, nullptr, &partition_set, parent));

  if (!conflict_entries.empty()) {
    std::vector<std::string> paths;
    paths.reserve(conflict_entries.size());
    for (const auto& entry : conflict_entries) {
      if (entry.data_file != nullptr) {
        paths.push_back(entry.data_file->file_path);
      }
    }
    return ValidationFailed(
        "Found conflicting files that can contain records matching partitions: {}",
        JoinStrings(paths, ", "));
  }

  return {};
}

Status MergingSnapshotProducer::ValidateAddedDataFiles(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> conflict_filter,
    const std::shared_ptr<Snapshot>& parent) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto conflict_entries,
      AddedDataFiles(base, starting_snapshot_id, conflict_filter, nullptr, parent));

  if (!conflict_entries.empty()) {
    std::vector<std::string> paths;
    paths.reserve(conflict_entries.size());
    for (const auto& entry : conflict_entries) {
      if (entry.data_file != nullptr) {
        paths.push_back(entry.data_file->file_path);
      }
    }
    return ValidationFailed(
        "Found conflicting files that can contain records matching {}: {}",
        conflict_filter->ToString(), JoinStrings(paths, ", "));
  }

  return {};
}

Status MergingSnapshotProducer::ValidateNoNewDeletesForDataFiles(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    std::span<const std::shared_ptr<DataFile>> data_files,
    const std::shared_ptr<Snapshot>& parent) {
  bool ignore_equality_deletes = impl_->new_data_files_data_sequence_number_.has_value();
  return ValidateNoNewDeletesForDataFilesInternal(
      base, starting_snapshot_id, nullptr, data_files, ignore_equality_deletes, parent);
}

Status MergingSnapshotProducer::ValidateNoNewDeletesForDataFiles(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter,
    std::span<const std::shared_ptr<DataFile>> data_files,
    const std::shared_ptr<Snapshot>& parent) {
  return ValidateNoNewDeletesForDataFilesInternal(base, starting_snapshot_id, data_filter,
                                                  data_files, false, parent);
}

Status MergingSnapshotProducer::ValidateNoNewDeletesForDataFilesInternal(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter,
    std::span<const std::shared_ptr<DataFile>> data_files, bool ignore_equality_deletes,
    const std::shared_ptr<Snapshot>& parent) {
  if (parent == nullptr || base.format_version < 2) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto deletes, AddedDeleteFiles(base, starting_snapshot_id,
                                                         data_filter, nullptr, parent));

  int64_t starting_seq = StartingSequenceNumber(base, starting_snapshot_id);
  for (const auto& data_file : data_files) {
    if (data_file == nullptr) continue;

    ICEBERG_ASSIGN_OR_RAISE(auto delete_files,
                            deletes->ForDataFile(starting_seq, *data_file));

    if (ignore_equality_deletes) {
      // Only fail on position deletes (e.g. RewriteFiles with same sequence number)
      bool has_pos_deletes =
          std::any_of(delete_files.begin(), delete_files.end(), [](const auto& df) {
            return df->content == DataFile::Content::kPositionDeletes;
          });
      if (has_pos_deletes) {
        return ValidationFailed(
            "Cannot commit, found new position delete for replaced data file: {}",
            data_file->file_path);
      }
    } else {
      // Fail on any delete
      if (!delete_files.empty()) {
        return ValidationFailed(
            "Cannot commit, found new delete for replaced data file: {}",
            data_file->file_path);
      }
    }
  }

  return {};
}

Status MergingSnapshotProducer::ValidateNoNewDeleteFiles(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> data_filter, const std::shared_ptr<Snapshot>& parent) {
  ICEBERG_ASSIGN_OR_RAISE(auto deletes, AddedDeleteFiles(base, starting_snapshot_id,
                                                         data_filter, nullptr, parent));

  if (!deletes->empty()) {
    auto files = deletes->ReferencedDeleteFiles();
    std::vector<std::string> paths;
    paths.reserve(files.size());
    for (const auto& file : files) {
      paths.push_back(file->file_path);
    }
    return ValidationFailed(
        "Found new conflicting delete files that can apply to records matching {}: {}",
        data_filter->ToString(), JoinStrings(paths, ", "));
  }

  return {};
}

Status MergingSnapshotProducer::ValidateNoNewDeleteFiles(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    const PartitionSet& partition_set, const std::shared_ptr<Snapshot>& parent) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto deletes,
      AddedDeleteFiles(base, starting_snapshot_id, nullptr, &partition_set, parent));

  if (!deletes->empty()) {
    auto files = deletes->ReferencedDeleteFiles();
    std::vector<std::string> paths;
    paths.reserve(files.size());
    for (const auto& file : files) {
      paths.push_back(file->file_path);
    }
    return ValidationFailed(
        "Found new conflicting delete files that can apply to records matching "
        "partitions: {}",
        JoinStrings(paths, ", "));
  }

  return {};
}

Status MergingSnapshotProducer::ValidateDataFilesExist(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    const std::unordered_set<std::string>& required_data_files,
    bool skip_delete_operations, std::shared_ptr<Expression> conflict_detection_filter,
    const std::shared_ptr<Snapshot>& parent) {
  if (parent == nullptr || required_data_files.empty()) {
    return {};
  }

  const auto& operations = skip_delete_operations
                               ? kValidateDataFilesExistSkipDeleteOperations
                               : kValidateDataFilesExistOperations;

  ICEBERG_ASSIGN_OR_RAISE(auto history,
                          GetValidationHistory(base, starting_snapshot_id, operations,
                                               ManifestContent::kData, parent));

  if (history.manifests.empty()) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema, base.Schema());
  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(transaction_->table()->io(), schema,
                          SpecsToMap(base.partition_specs), history.manifests));

  manifest_group->CaseSensitive(impl_->case_sensitive_);
  manifest_group->FilterManifestEntries(
      [&history, &required_data_files](const ManifestEntry& entry) {
        if (entry.status == ManifestStatus::kAdded) {
          return false;
        }
        if (!entry.snapshot_id.has_value()) {
          return false;
        }
        if (!history.snapshot_ids.contains(entry.snapshot_id.value())) {
          return false;
        }
        if (entry.data_file == nullptr) {
          return false;
        }
        return required_data_files.contains(entry.data_file->file_path);
      });
  manifest_group->IgnoreExisting();

  if (conflict_detection_filter != nullptr) {
    manifest_group->FilterData(conflict_detection_filter);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto deleted_entries, manifest_group->Entries());

  if (!deleted_entries.empty()) {
    std::vector<std::string> missing_paths;
    missing_paths.reserve(deleted_entries.size());
    for (const auto& entry : deleted_entries) {
      if (entry.data_file != nullptr) {
        missing_paths.push_back(entry.data_file->file_path);
      }
    }
    return ValidationFailed("Cannot commit, missing data files: {}",
                            JoinStrings(missing_paths, ", "));
  }

  return {};
}

Status MergingSnapshotProducer::ValidateAddedDVs(
    const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
    std::shared_ptr<Expression> conflict_detection_filter,
    const std::shared_ptr<Snapshot>& parent) {
  if (parent == nullptr || impl_->new_dv_refs_.empty()) {
    return {};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto history,
      GetValidationHistory(base, starting_snapshot_id, kValidateAddedDVsOperations,
                           ManifestContent::kDeletes, parent));

  for (const auto& manifest : history.manifests) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema, base.Schema());
    ICEBERG_ASSIGN_OR_RAISE(auto spec,
                            base.PartitionSpecById(manifest.partition_spec_id));
    ICEBERG_ASSIGN_OR_RAISE(
        auto reader,
        ManifestReader::Make(manifest, transaction_->table()->io(), schema, spec));

    ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());

    for (const auto& entry : entries) {
      if (!entry.snapshot_id.has_value() ||
          !history.snapshot_ids.contains(entry.snapshot_id.value())) {
        continue;
      }
      if (entry.data_file == nullptr) {
        continue;
      }

      if (ContentFileUtil::IsDV(*entry.data_file)) {
        auto ref_result = ContentFileUtil::ReferencedDataFile(*entry.data_file);
        if (ref_result.has_value() && ref_result.value().has_value()) {
          const std::string& ref_path = ref_result.value().value();
          if (impl_->new_dv_refs_.contains(ref_path)) {
            return ValidationFailed("Found concurrently added DV for {}: {}", ref_path,
                                    ContentFileUtil::DVDesc(*entry.data_file));
          }
        }
      }
    }
  }

  return {};
}

// ============================================================================
// Apply and Summary
// ============================================================================

Result<std::vector<ManifestFile>> MergingSnapshotProducer::PrepareNewDataManifests() {
  std::vector<ManifestFile> new_manifests;
  int64_t snapshot_id = SnapshotId();

  if (!impl_->new_data_files_by_spec_.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto data_file_manifests, NewDataFilesAsManifests());
    new_manifests.insert(new_manifests.end(),
                         std::make_move_iterator(data_file_manifests.begin()),
                         std::make_move_iterator(data_file_manifests.end()));
    new_manifests.insert(new_manifests.end(), impl_->append_manifests_.begin(),
                         impl_->append_manifests_.end());
    new_manifests.insert(
        new_manifests.end(),
        std::make_move_iterator(impl_->rewritten_append_manifests_.begin()),
        std::make_move_iterator(impl_->rewritten_append_manifests_.end()));
  } else {
    new_manifests.insert(new_manifests.end(), impl_->append_manifests_.begin(),
                         impl_->append_manifests_.end());
    new_manifests.insert(
        new_manifests.end(),
        std::make_move_iterator(impl_->rewritten_append_manifests_.begin()),
        std::make_move_iterator(impl_->rewritten_append_manifests_.end()));
  }

  // Set snapshot ID on all new manifests
  for (auto& manifest : new_manifests) {
    if (manifest.added_snapshot_id == kInvalidSnapshotId) {
      manifest.added_snapshot_id = snapshot_id;
    }
  }

  return new_manifests;
}

Result<std::vector<ManifestFile>> MergingSnapshotProducer::PrepareDeleteManifests() {
  if (impl_->new_delete_files_by_spec_.empty()) {
    return std::vector<ManifestFile>{};
  }
  return NewDeleteFilesAsManifests();
}

Result<std::vector<ManifestFile>> MergingSnapshotProducer::NewDataFilesAsManifests() {
  if (impl_->has_new_data_files_ && !impl_->cached_new_data_manifests_.empty()) {
    for (const auto& manifest : impl_->cached_new_data_manifests_) {
      std::ignore = DeleteFile(manifest.manifest_path);
    }
    impl_->cached_new_data_manifests_.clear();
  }

  if (impl_->cached_new_data_manifests_.empty()) {
    for (const auto& [spec_id, data_files] : impl_->new_data_files_by_spec_) {
      ICEBERG_ASSIGN_OR_RAISE(auto spec, Spec(spec_id));
      ICEBERG_ASSIGN_OR_RAISE(
          auto written_manifests,
          WriteDataManifests(data_files.as_span(), spec,
                             impl_->new_data_files_data_sequence_number_));
      impl_->cached_new_data_manifests_.insert(
          impl_->cached_new_data_manifests_.end(),
          std::make_move_iterator(written_manifests.begin()),
          std::make_move_iterator(written_manifests.end()));
    }
    impl_->has_new_data_files_ = false;
  }

  return impl_->cached_new_data_manifests_;
}

Result<std::vector<ManifestFile>> MergingSnapshotProducer::NewDeleteFilesAsManifests() {
  if (impl_->has_new_delete_files_ && !impl_->cached_new_delete_manifests_.empty()) {
    for (const auto& manifest : impl_->cached_new_delete_manifests_) {
      std::ignore = DeleteFile(manifest.manifest_path);
    }
    impl_->cached_new_delete_manifests_.clear();
  }

  if (impl_->cached_new_delete_manifests_.empty()) {
    for (const auto& [spec_id, delete_files] : impl_->new_delete_files_by_spec_) {
      ICEBERG_ASSIGN_OR_RAISE(auto spec, Spec(spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto written_manifests,
                              WriteDeleteManifests(delete_files, spec));
      impl_->cached_new_delete_manifests_.insert(
          impl_->cached_new_delete_manifests_.end(),
          std::make_move_iterator(written_manifests.begin()),
          std::make_move_iterator(written_manifests.end()));
    }
    impl_->has_new_delete_files_ = false;
  }

  return impl_->cached_new_delete_manifests_;
}

Result<std::vector<ManifestFile>> MergingSnapshotProducer::Apply(
    const TableMetadata& metadata_to_update, const std::shared_ptr<Snapshot>& snapshot) {
  // Get the schema for the target branch
  ICEBERG_ASSIGN_OR_RAISE(auto schema,
                          SnapshotUtil::SchemaFor(metadata_to_update, target_branch()));

  // Filter existing data manifests
  std::vector<ManifestFile> data_manifests;
  if (snapshot != nullptr) {
    auto cached = SnapshotCache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto data_manifests_span,
                            cached.DataManifests(transaction_->table()->io()));
    data_manifests.assign(data_manifests_span.begin(), data_manifests_span.end());
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto filtered_data_manifests,
      impl_->data_filter_manager_->FilterManifests(schema, data_manifests));

  // Calculate min sequence number from filtered data manifests
  int64_t min_data_sequence_number = metadata_to_update.last_sequence_number;
  for (const auto& manifest : filtered_data_manifests) {
    if (manifest.min_sequence_number != kInvalidSequenceNumber) {
      min_data_sequence_number =
          std::min(min_data_sequence_number, manifest.min_sequence_number);
    }
  }

  impl_->delete_filter_manager_->DropDeleteFilesOlderThan(min_data_sequence_number);

  // Pass data files to be deleted to remove orphaned DVs from delete manifests
  impl_->delete_filter_manager_->RemoveDanglingDeletesFor(
      impl_->data_filter_manager_->FilesToDelete());

  // Filter delete manifests
  std::vector<ManifestFile> delete_manifests;
  if (snapshot != nullptr) {
    auto cached = SnapshotCache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto delete_manifests_span,
                            cached.DeleteManifests(transaction_->table()->io()));
    delete_manifests.assign(delete_manifests_span.begin(), delete_manifests_span.end());
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto filtered_delete_manifests,
      impl_->delete_filter_manager_->FilterManifests(schema, delete_manifests));

  // Prepare new manifests
  ICEBERG_ASSIGN_OR_RAISE(auto new_data_manifests, PrepareNewDataManifests());
  ICEBERG_ASSIGN_OR_RAISE(auto new_delete_manifests, PrepareDeleteManifests());

  // Combine all manifests (new first, then filtered - match Java Iterables.concat order)
  std::vector<ManifestFile> all_data_manifests;
  all_data_manifests.reserve(new_data_manifests.size() + filtered_data_manifests.size());
  all_data_manifests.insert(all_data_manifests.end(),
                            std::make_move_iterator(new_data_manifests.begin()),
                            std::make_move_iterator(new_data_manifests.end()));
  all_data_manifests.insert(all_data_manifests.end(),
                            std::make_move_iterator(filtered_data_manifests.begin()),
                            std::make_move_iterator(filtered_data_manifests.end()));

  std::vector<ManifestFile> all_delete_manifests;
  all_delete_manifests.reserve(new_delete_manifests.size() +
                               filtered_delete_manifests.size());
  all_delete_manifests.insert(all_delete_manifests.end(),
                              std::make_move_iterator(new_delete_manifests.begin()),
                              std::make_move_iterator(new_delete_manifests.end()));
  all_delete_manifests.insert(all_delete_manifests.end(),
                              std::make_move_iterator(filtered_delete_manifests.begin()),
                              std::make_move_iterator(filtered_delete_manifests.end()));

  // Filter out empty manifests and those with only deleted entries
  auto should_keep = [snapshot_id = SnapshotId()](const ManifestFile& manifest) {
    return manifest.has_added_files() || manifest.has_existing_files() ||
           manifest.added_snapshot_id == snapshot_id;
  };

  std::vector<ManifestFile> kept_data_manifests;
  for (auto& manifest : all_data_manifests) {
    if (should_keep(manifest)) {
      kept_data_manifests.push_back(std::move(manifest));
    }
  }

  std::vector<ManifestFile> kept_delete_manifests;
  for (auto& manifest : all_delete_manifests) {
    if (should_keep(manifest)) {
      kept_delete_manifests.push_back(std::move(manifest));
    }
  }

  // Merge manifests (first manifest is from new data/appends - used for minCountToMerge)
  std::optional<std::string_view> first_data_path =
      kept_data_manifests.empty()
          ? std::nullopt
          : std::optional<std::string_view>(kept_data_manifests[0].manifest_path);
  std::optional<std::string_view> first_delete_path =
      kept_delete_manifests.empty()
          ? std::nullopt
          : std::optional<std::string_view>(kept_delete_manifests[0].manifest_path);
  ICEBERG_ASSIGN_OR_RAISE(
      auto merged_data_manifests,
      impl_->data_merge_manager_->MergeManifests(kept_data_manifests, first_data_path));
  ICEBERG_ASSIGN_OR_RAISE(auto merged_delete_manifests,
                          impl_->delete_merge_manager_->MergeManifests(
                              kept_delete_manifests, first_delete_path));

  // Combine data and delete manifests
  std::vector<ManifestFile> result;
  result.reserve(merged_data_manifests.size() + merged_delete_manifests.size());
  result.insert(result.end(), std::make_move_iterator(merged_data_manifests.begin()),
                std::make_move_iterator(merged_data_manifests.end()));
  result.insert(result.end(), std::make_move_iterator(merged_delete_manifests.begin()),
                std::make_move_iterator(merged_delete_manifests.end()));

  // Update summary builder
  summary_builder().Clear();
  summary_builder().Merge(impl_->added_files_summary_);
  summary_builder().Merge(impl_->appended_manifests_summary_);
  summary_builder().Merge(
      impl_->data_filter_manager_->BuildSummary(filtered_data_manifests));
  summary_builder().Merge(
      impl_->delete_filter_manager_->BuildSummary(filtered_delete_manifests));

  // Build manifest count summary (match Java buildManifestCountSummary)
  int64_t snapshot_id = SnapshotId();
  int32_t manifests_created = 0;
  int32_t manifests_kept = 0;
  for (const auto& manifest : result) {
    if (snapshot_id == manifest.added_snapshot_id) {
      manifests_created++;
    } else if (manifest.added_snapshot_id != kInvalidSnapshotId) {
      manifests_kept++;
    }
  }
  int32_t replaced_count = impl_->data_merge_manager_->ReplacedManifestsCount() +
                           impl_->delete_merge_manager_->ReplacedManifestsCount();
  summary_builder().Set(SnapshotSummaryFields::kManifestsCreated,
                        std::to_string(manifests_created));
  summary_builder().Set(SnapshotSummaryFields::kManifestsKept,
                        std::to_string(manifests_kept));
  summary_builder().Set(SnapshotSummaryFields::kManifestsReplaced,
                        std::to_string(replaced_count));

  return result;
}

std::unordered_map<std::string, std::string> MergingSnapshotProducer::Summary() {
  summary_builder().SetPartitionSummaryLimit(
      base().properties.Get(TableProperties::kWritePartitionSummaryLimit));
  return summary_builder().Build();
}

void MergingSnapshotProducer::CleanUncommitted(
    const std::unordered_set<std::string>& committed) {
  impl_->data_merge_manager_->CleanUncommitted(committed);
  impl_->data_filter_manager_->CleanUncommitted(committed);
  impl_->delete_merge_manager_->CleanUncommitted(committed);
  impl_->delete_filter_manager_->CleanUncommitted(committed);

  // Clean up new data manifests
  if (!impl_->cached_new_data_manifests_.empty()) {
    for (const auto& manifest : impl_->cached_new_data_manifests_) {
      if (!committed.contains(manifest.manifest_path)) {
        std::ignore = DeleteFile(manifest.manifest_path);
      }
    }
    impl_->cached_new_data_manifests_.clear();
  }

  // Clean up new delete manifests
  if (!impl_->cached_new_delete_manifests_.empty()) {
    for (const auto& manifest : impl_->cached_new_delete_manifests_) {
      if (!committed.contains(manifest.manifest_path)) {
        std::ignore = DeleteFile(manifest.manifest_path);
      }
    }
    impl_->cached_new_delete_manifests_.clear();
  }

  // Rewritten manifests are always owned by the table
  if (!impl_->rewritten_append_manifests_.empty()) {
    for (const auto& manifest : impl_->rewritten_append_manifests_) {
      if (!committed.contains(manifest.manifest_path)) {
        std::ignore = DeleteFile(manifest.manifest_path);
      }
    }
  }

  // Append manifests are only owned if commit succeeded
  if (!committed.empty()) {
    for (const auto& manifest : impl_->append_manifests_) {
      if (!committed.contains(manifest.manifest_path)) {
        std::ignore = DeleteFile(manifest.manifest_path);
      }
    }
  }
}

bool MergingSnapshotProducer::CleanupAfterCommit() const { return true; }

}  // namespace iceberg
