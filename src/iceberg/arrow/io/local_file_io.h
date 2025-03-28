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

#include <memory>

#include <arrow/filesystem/localfs.h>

#include "iceberg/file_io.h"
#include "iceberg/iceberg_bundle_export.h"

namespace iceberg::arrow::io {

/// \brief A concrete implementation of FileIO for file system.
class ICEBERG_BUNDLE_EXPORT LocalFileIO : public FileIO {
 public:
  explicit LocalFileIO(std::shared_ptr<::arrow::fs::LocalFileSystem>& local_fs)
      : local_fs_(local_fs) {}

  ~LocalFileIO() override = default;

  /// \brief Read the content of the file at the given location.
  expected<std::string, Error> ReadFile(const std::string& file_location,
                                        std::optional<size_t> length) override;

  /// \brief Write the given content to the file at the given location.
  expected<void, Error> WriteFile(const std::string& file_location,
                                  std::string_view content, bool overwrite) override;

  /// \brief Delete a file at the given location.
  expected<void, Error> DeleteFile(const std::string& file_location) override;

 private:
  /// \brief Check if a file exists
  bool FileExists(const std::string& location);

  std::shared_ptr<::arrow::fs::LocalFileSystem>& local_fs_;
};

}  // namespace iceberg::arrow::io
