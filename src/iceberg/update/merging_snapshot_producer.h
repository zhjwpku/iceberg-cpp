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

/// \file iceberg/update/merging_snapshot_producer.h
/// \brief Base class for snapshot producers that merge and filter manifests.

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "iceberg/delete_file_index.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/type_fwd.h"
#include "iceberg/update/snapshot_update.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/partition_value_util.h"

namespace iceberg {

/// \brief Manages filtering of manifests for deletes and rewrites.
///
/// This class tracks files to be deleted and handles rewriting manifests
/// to remove those files.
class ICEBERG_EXPORT ManifestFilterManager {
 public:
  /// \brief Construct a ManifestFilterManager.
  ///
  /// \param specs_by_id Partition specs by their IDs
  /// \param io FileIO for reading and writing manifests
  /// \param snapshot_id The snapshot ID for new manifest entries
  ManifestFilterManager(
      std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_by_id,
      std::shared_ptr<FileIO> io, int64_t snapshot_id);

  ~ManifestFilterManager();

  // Non-copyable, movable
  ManifestFilterManager(const ManifestFilterManager&) = delete;
  ManifestFilterManager& operator=(const ManifestFilterManager&) = delete;
  ManifestFilterManager(ManifestFilterManager&&) noexcept;
  ManifestFilterManager& operator=(ManifestFilterManager&&) noexcept;

  /// \brief Set case sensitivity for column name matching.
  ManifestFilterManager& CaseSensitive(bool case_sensitive);

  /// \brief Delete a specific data file by path.
  void Delete(const std::string& path);

  /// \brief Delete a specific data file.
  void Delete(const DataFile& file);

  /// \brief Delete files matching the row filter expression.
  void DeleteByRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Drop all files in a specific partition.
  void DropPartition(int32_t spec_id, const PartitionValues& partition);

  /// \brief Fail if any deletes are found during filtering.
  void FailAnyDelete();

  /// \brief Fail if delete paths are missing.
  void FailMissingDeletePaths();

  /// \brief Set the minimum sequence number for delete files.
  ///
  /// Delete files with sequence number older than this will be removed when
  /// filtering delete manifests. Used to drop delete files that can no longer
  /// match any existing rows.
  void DropDeleteFilesOlderThan(int64_t sequence_number);

  /// \brief Mark data file paths that are being deleted.
  ///
  /// When filtering delete manifests, DVs that reference these paths will be
  /// removed as orphaned (dangling) deletes.
  void RemoveDanglingDeletesFor(const std::unordered_set<std::string>& data_file_paths);

  /// \brief Check if this manager contains any deletes.
  bool ContainsDeletes() const;

  /// \brief Get the set of files to be deleted.
  const std::unordered_set<std::string>& FilesToDelete() const;

  /// \brief Filter manifests and return the filtered list.
  ///
  /// \param schema The schema to use for filtering
  /// \param manifests The manifests to filter
  /// \return Filtered manifests
  Result<std::vector<ManifestFile>> FilterManifests(
      const std::shared_ptr<Schema>& schema, std::span<const ManifestFile> manifests);

  /// \brief Build a summary of changes from the filtered manifests.
  SnapshotSummaryBuilder BuildSummary(std::span<const ManifestFile> filtered);

  /// \brief Clean up uncommitted manifests.
  void CleanUncommitted(const std::unordered_set<std::string>& committed);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

/// \brief Abstract base for merging manifest files (aligns with Java
/// ManifestMergeManager).
///
/// Uses bin packing to combine small manifests. Subclasses implement the abstract
/// methods to provide manifest I/O. DataMergeManager and DeleteMergeManager in
/// MergingSnapshotProducer extend this for data and delete manifests.
class ICEBERG_EXPORT ManifestMergeManager {
 public:
  /// \brief Construct a ManifestMergeManager.
  ManifestMergeManager(int64_t target_size_bytes, int32_t min_count_to_merge,
                       bool merge_enabled, ManifestContent content);

  virtual ~ManifestMergeManager() = default;

  /// \brief Subclasses implement: return the snapshot ID for this commit.
  virtual int64_t SnapshotId() = 0;
  /// \brief Subclasses implement: return the partition spec by ID.
  virtual Result<std::shared_ptr<PartitionSpec>> Spec(int32_t spec_id) = 0;
  /// \brief Subclasses implement: delete a file at the given path.
  virtual Status DeleteFile(const std::string& path) = 0;
  /// \brief Subclasses implement: create a manifest writer for the given spec.
  virtual Result<std::unique_ptr<ManifestWriter>> NewManifestWriter(
      const std::shared_ptr<PartitionSpec>& spec) = 0;
  /// \brief Subclasses implement: create a manifest reader for the given manifest.
  virtual Result<std::unique_ptr<ManifestReader>> NewManifestReader(
      const ManifestFile& manifest) = 0;

  /// \brief Merge manifests using bin packing.
  ///
  /// \param manifests Manifests to merge (new manifests first, then filtered)
  /// \param first_manifest_path Path of the first manifest (new/in-memory); when a bin
  ///        contains this manifest and has fewer than min_count_to_merge manifests, the
  ///        bin is not merged (to avoid merging small bins with new data)
  Result<std::vector<ManifestFile>> MergeManifests(
      const std::vector<ManifestFile>& manifests,
      std::optional<std::string_view> first_manifest_path = std::nullopt);

  /// \brief Count of manifests replaced (merged) during bin-packing.
  int32_t ReplacedManifestsCount() const { return replaced_manifests_count_; }

  /// \brief Clean up uncommitted merged manifests.
  void CleanUncommitted(const std::unordered_set<std::string>& committed);

 private:
  std::vector<std::vector<ManifestFile>> PackManifests(
      const std::vector<ManifestFile>& manifests) const;

  Result<ManifestFile> MergeManifestGroup(const std::vector<ManifestFile>& group);

  int64_t target_size_bytes_;
  int32_t min_count_to_merge_;
  bool merge_enabled_;
  ManifestContent content_;

  /// Cache: bin hash -> (bin, merged manifest). Bin stored for cleanUncommitted.
  std::unordered_map<size_t, std::pair<std::vector<ManifestFile>, ManifestFile>>
      merged_manifests_cache_;
  int32_t replaced_manifests_count_{0};
};

/// \brief Base class for snapshot producers that merge manifests and validate
/// concurrent changes.
///
/// MergingSnapshotProducer handles operations that add or remove data files
/// and delete files, with support for manifest merging and validation of
/// concurrent modifications.
class ICEBERG_EXPORT MergingSnapshotProducer : public SnapshotUpdate {
 public:
  /// MergingSnapshotProducer is an abstract base class.
  /// Derived classes should implement the operation() method.
  ~MergingSnapshotProducer() override;

  /// \brief Set case sensitivity for column name matching.
  ///
  /// \param case_sensitive Whether matching should be case sensitive
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& CaseSensitive(bool case_sensitive);

  /// \brief Delete a file by its path.
  ///
  /// \param path The file path to delete
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& Delete(const std::string& path);

  /// \brief Delete a specific data file.
  ///
  /// \param file The data file to delete
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& Delete(const DataFile& file);

  /// \brief Delete a specific delete file.
  ///
  /// \param file The delete file to delete
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& DeleteDeleteFile(const DataFile& file);

  /// \brief Delete files matching the given expression.
  ///
  /// \param expr The expression to match rows for deletion
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& DeleteByRowFilter(std::shared_ptr<Expression> expr);

  /// \brief Drop all files in a partition.
  ///
  /// \param spec_id The partition spec ID
  /// \param partition The partition values
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& DropPartition(int32_t spec_id,
                                         const PartitionValues& partition);

  /// \brief Add a data file to the new snapshot.
  ///
  /// \param file The data file to add
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& Add(const std::shared_ptr<DataFile>& file);

  /// \brief Add a delete file to the new snapshot.
  ///
  /// \param file The delete file to add
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& AddDelete(const std::shared_ptr<DataFile>& file);

  /// \brief Add a delete file with a specific data sequence number.
  ///
  /// \param file The delete file to add
  /// \param data_sequence_number The data sequence number
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& AddDelete(const std::shared_ptr<DataFile>& file,
                                     int64_t data_sequence_number);

  /// \brief Add a manifest to the new snapshot.
  ///
  /// \param manifest The manifest to add
  /// \return Reference to this for method chaining
  MergingSnapshotProducer& AddManifest(const ManifestFile& manifest);

  /// \brief Check if this operation deletes any data files.
  bool DeletesDataFiles() const;

  /// \brief Check if this operation deletes any delete files.
  bool DeletesDeleteFiles() const;

  /// \brief Check if this operation adds any data files.
  bool AddsDataFiles() const;

  /// \brief Check if this operation adds any delete files.
  bool AddsDeleteFiles() const;

  /// \brief Validate that no conflicting data files were added.
  ///
  /// Checks that no files matching the given partitions have been added
  /// to the table since the starting snapshot.
  ///
  /// \param base The table metadata to validate against
  /// \param starting_snapshot_id The starting snapshot ID
  /// \param partition_set Set of partitions to check
  /// \param parent The parent snapshot being validated
  /// \return Status indicating success or validation failure
  Status ValidateAddedDataFiles(const TableMetadata& base,
                                std::optional<int64_t> starting_snapshot_id,
                                const PartitionSet& partition_set,
                                const std::shared_ptr<Snapshot>& parent);

  /// \brief Validate that no conflicting data files were added.
  ///
  /// Checks that no files matching the given filter have been added
  /// to the table since the starting snapshot.
  ///
  /// \param base The table metadata to validate against
  /// \param starting_snapshot_id The starting snapshot ID
  /// \param conflict_filter Filter expression for conflicting files
  /// \param parent The parent snapshot being validated
  /// \return Status indicating success or validation failure
  Status ValidateAddedDataFiles(const TableMetadata& base,
                                std::optional<int64_t> starting_snapshot_id,
                                std::shared_ptr<Expression> conflict_filter,
                                const std::shared_ptr<Snapshot>& parent);

  /// \brief Validate that no new delete files affect the given data files.
  ///
  /// \param base The table metadata to validate against
  /// \param starting_snapshot_id The starting snapshot ID
  /// \param data_files Data files to validate
  /// \param parent The parent snapshot being validated
  /// \return Status indicating success or validation failure
  Status ValidateNoNewDeletesForDataFiles(
      const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
      std::span<const std::shared_ptr<DataFile>> data_files,
      const std::shared_ptr<Snapshot>& parent);

  /// \brief Validate that no new delete files affect the given data files.
  ///
  /// \param base The table metadata to validate against
  /// \param starting_snapshot_id The starting snapshot ID
  /// \param data_filter Data filter expression
  /// \param data_files Data files to validate
  /// \param parent The parent snapshot being validated
  /// \return Status indicating success or validation failure
  Status ValidateNoNewDeletesForDataFiles(
      const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
      std::shared_ptr<Expression> data_filter,
      std::span<const std::shared_ptr<DataFile>> data_files,
      const std::shared_ptr<Snapshot>& parent);

  /// \brief Validate that no new delete files matching the filter were added.
  ///
  /// \param base The table metadata to validate against
  /// \param starting_snapshot_id The starting snapshot ID
  /// \param data_filter Filter expression for delete files
  /// \param parent The parent snapshot being validated
  /// \return Status indicating success or validation failure
  Status ValidateNoNewDeleteFiles(const TableMetadata& base,
                                  std::optional<int64_t> starting_snapshot_id,
                                  std::shared_ptr<Expression> data_filter,
                                  const std::shared_ptr<Snapshot>& parent);

  /// \brief Validate that no new delete files matching the partitions were added.
  ///
  /// \param base The table metadata to validate against
  /// \param starting_snapshot_id The starting snapshot ID
  /// \param partition_set Partition set for delete files
  /// \param parent The parent snapshot being validated
  /// \return Status indicating success or validation failure
  Status ValidateNoNewDeleteFiles(const TableMetadata& base,
                                  std::optional<int64_t> starting_snapshot_id,
                                  const PartitionSet& partition_set,
                                  const std::shared_ptr<Snapshot>& parent);

  /// \brief Validate that no required data files were deleted.
  ///
  /// \param base The table metadata to validate against
  /// \param starting_snapshot_id The starting snapshot ID
  /// \param required_data_files Set of required data file paths
  /// \param skip_delete_operations Whether to skip delete operations
  /// \param conflict_detection_filter Filter for conflict detection
  /// \param parent The parent snapshot being validated
  /// \return Status indicating success or validation failure
  Status ValidateDataFilesExist(
      const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
      const std::unordered_set<std::string>& required_data_files,
      bool skip_delete_operations, std::shared_ptr<Expression> conflict_detection_filter,
      const std::shared_ptr<Snapshot>& parent);

  /// \brief Validate that no DVs were added for referenced data files.
  ///
  /// \param base The table metadata to validate against
  /// \param starting_snapshot_id The starting snapshot ID
  /// \param conflict_detection_filter Filter for conflict detection
  /// \param parent The parent snapshot being validated
  /// \return Status indicating success or validation failure
  Status ValidateAddedDVs(const TableMetadata& base,
                          std::optional<int64_t> starting_snapshot_id,
                          std::shared_ptr<Expression> conflict_detection_filter,
                          const std::shared_ptr<Snapshot>& parent);

  /// \brief Get the delete expression used for row filtering.
  std::shared_ptr<Expression> RowFilter() const;

  /// \brief Set the data sequence number for new data files.
  ///
  /// \param sequence_number The sequence number to use
  void SetNewDataFilesDataSequenceNumber(int64_t sequence_number);

  /// \brief Get the data files that have been added.
  std::vector<std::shared_ptr<DataFile>> AddedDataFiles() const;

  /// \brief Fail if any deletes are encountered.
  void FailAnyDelete();

  /// \brief Fail if delete paths are missing.
  void FailMissingDeletePaths();

  // Overrides from SnapshotUpdate
  Result<std::vector<ManifestFile>> Apply(
      const TableMetadata& metadata_to_update,
      const std::shared_ptr<Snapshot>& snapshot) override;
  std::unordered_map<std::string, std::string> Summary() override;
  void CleanUncommitted(const std::unordered_set<std::string>& committed) override;
  bool CleanupAfterCommit() const override;

 protected:
  explicit MergingSnapshotProducer(std::string table_name,
                                   std::shared_ptr<Transaction> transaction);

  /// \brief Validate the delete file according to format version.
  Status ValidateNewDeleteFile(const DataFile& file);

  /// \brief Internal implementation for adding delete files.
  MergingSnapshotProducer& AddDeleteInternal(const std::shared_ptr<DataFile>& file,
                                             std::optional<int64_t> data_sequence_number);

 protected:
  /// \brief Get the partition spec by ID (used by merge managers).
  Result<std::shared_ptr<PartitionSpec>> Spec(int32_t spec_id);

  /// \brief Create a manifest writer for data files (used by DataMergeManager).
  Result<std::unique_ptr<ManifestWriter>> NewManifestWriter(
      const std::shared_ptr<PartitionSpec>& spec);
  /// \brief Create a manifest writer for delete files (used by DeleteMergeManager).
  Result<std::unique_ptr<ManifestWriter>> NewDeleteManifestWriter(
      const std::shared_ptr<PartitionSpec>& spec);
  /// \brief Create a manifest reader for data manifests (used by DataMergeManager).
  Result<std::unique_ptr<ManifestReader>> NewManifestReader(const ManifestFile& manifest);
  /// \brief Create a manifest reader for delete manifests (used by DeleteMergeManager).
  Result<std::unique_ptr<ManifestReader>> NewDeleteManifestReader(
      const ManifestFile& manifest);

 private:
  /// \brief Internal implementation with ignoreEqualityDeletes parameter.
  Status ValidateNoNewDeletesForDataFilesInternal(
      const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
      std::shared_ptr<Expression> data_filter,
      std::span<const std::shared_ptr<DataFile>> data_files, bool ignore_equality_deletes,
      const std::shared_ptr<Snapshot>& parent);

  /// \brief Copy a manifest with a new snapshot ID.
  Result<ManifestFile> CopyManifest(const ManifestFile& manifest);

  /// \brief Prepare new data manifests.
  Result<std::vector<ManifestFile>> PrepareNewDataManifests();

  /// \brief Prepare new delete manifests.
  Result<std::vector<ManifestFile>> PrepareDeleteManifests();

  /// \brief Write manifests for new data files.
  Result<std::vector<ManifestFile>> NewDataFilesAsManifests();

  /// \brief Write manifests for new delete files.
  Result<std::vector<ManifestFile>> NewDeleteFilesAsManifests();

  /// \brief Get delete files added since starting snapshot.
  Result<std::unique_ptr<DeleteFileIndex>> AddedDeleteFiles(
      const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
      std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set,
      const std::shared_ptr<Snapshot>& parent);

  /// \brief Get data files added since starting snapshot.
  Result<std::vector<ManifestEntry>> AddedDataFiles(
      const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
      std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set,
      const std::shared_ptr<Snapshot>& parent);

  /// \brief Get deleted data files since starting snapshot.
  Result<std::vector<ManifestEntry>> DeletedDataFiles(
      const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
      std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set,
      const std::shared_ptr<Snapshot>& parent);

  /// \brief Compute the starting sequence number.
  int64_t StartingSequenceNumber(const TableMetadata& metadata,
                                 std::optional<int64_t> starting_snapshot_id);

  /// \brief Build a DeleteFileIndex from manifests.
  Result<std::unique_ptr<DeleteFileIndex>> BuildDeleteFileIndex(
      const std::vector<ManifestFile>& delete_manifests, int64_t starting_sequence_number,
      std::shared_ptr<Expression> data_filter, const PartitionSet* partition_set);

  /// \brief Get validation history (manifests and snapshot IDs).
  struct ValidationHistory {
    std::vector<ManifestFile> manifests;
    std::unordered_set<int64_t> snapshot_ids;
  };

  Result<ValidationHistory> GetValidationHistory(
      const TableMetadata& base, std::optional<int64_t> starting_snapshot_id,
      const std::unordered_set<std::string>& operations, ManifestContent content,
      const std::shared_ptr<Snapshot>& parent);

  class Impl;

  /// \brief Merge manager for data manifests (aligns with Java DataFileMergeManager).
  class DataMergeManager : public ManifestMergeManager {
   public:
    DataMergeManager(int64_t target_size_bytes, int32_t min_count_to_merge,
                     bool merge_enabled, MergingSnapshotProducer* parent);
    int64_t SnapshotId() override;
    Result<std::shared_ptr<PartitionSpec>> Spec(int32_t spec_id) override;
    Status DeleteFile(const std::string& path) override;
    Result<std::unique_ptr<ManifestWriter>> NewManifestWriter(
        const std::shared_ptr<PartitionSpec>& spec) override;
    Result<std::unique_ptr<ManifestReader>> NewManifestReader(
        const ManifestFile& manifest) override;

   private:
    MergingSnapshotProducer* parent_;
  };

  /// \brief Merge manager for delete manifests (aligns with Java DeleteFileMergeManager).
  class DeleteMergeManager : public ManifestMergeManager {
   public:
    DeleteMergeManager(int64_t target_size_bytes, int32_t min_count_to_merge,
                       bool merge_enabled, MergingSnapshotProducer* parent);
    int64_t SnapshotId() override;
    Result<std::shared_ptr<PartitionSpec>> Spec(int32_t spec_id) override;
    Status DeleteFile(const std::string& path) override;
    Result<std::unique_ptr<ManifestWriter>> NewManifestWriter(
        const std::shared_ptr<PartitionSpec>& spec) override;
    Result<std::unique_ptr<ManifestReader>> NewManifestReader(
        const ManifestFile& manifest) override;

   private:
    MergingSnapshotProducer* parent_;
  };

  friend class DataMergeManager;
  friend class DeleteMergeManager;

  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg
