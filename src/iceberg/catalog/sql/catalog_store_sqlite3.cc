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

#include <sqlite3.h>

#include <exception>
#include <memory>
#include <string_view>

#include <sqlpp23/sqlite3/sqlite3.h>

#include "iceberg/catalog/sql/catalog_store.h"
#include "iceberg/catalog/sql/catalog_store_sqlpp23_internal.h"
#include "iceberg/result.h"

namespace iceberg::sql {

namespace {

/// Recognizes SQLite primary-key / unique-constraint violations. With extended
/// result codes enabled the connection reports SQLITE_CONSTRAINT_PRIMARYKEY /
/// SQLITE_CONSTRAINT_UNIQUE; the primary SQLITE_CONSTRAINT is matched too as a
/// safety net.
struct SqliteTraits {
  static bool IsUniqueViolation(const std::exception& error) {
    const auto* sqlite_error = dynamic_cast<const sqlpp::sqlite3::exception*>(&error);
    if (sqlite_error == nullptr) {
      return false;
    }
    const int code = sqlite_error->error_code();
    return code == SQLITE_CONSTRAINT || code == SQLITE_CONSTRAINT_PRIMARYKEY ||
           code == SQLITE_CONSTRAINT_UNIQUE;
  }

  static bool IsDuplicateColumn(const std::exception& error) {
    const auto* sqlite_error = dynamic_cast<const sqlpp::sqlite3::exception*>(&error);
    return sqlite_error != nullptr &&
           std::string_view(sqlite_error->what()).find("duplicate column name") !=
               std::string_view::npos;
  }
};

using SqliteSingleCatalogStore =
    Sqlpp23CatalogStore<SingleConnectionSource<sqlpp::sqlite3::connection>, SqliteTraits>;

}  // namespace

Result<std::shared_ptr<CatalogStore>> MakeSqliteCatalogStore(
    const CatalogStoreOptions& options) {
  auto config = std::make_shared<sqlpp::sqlite3::connection_config>();
  config->path_to_database = options.uri.empty() ? ":memory:" : options.uri;
  config->flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  // Required so unique/primary-key violations surface their extended codes.
  config->use_extended_result_codes = true;

  // SQLite always uses a single connection regardless of `max_connections`: a
  // `:memory:` database is private to each connection, and a file database only
  // allows one writer, so a pool of write connections would just hit
  // SQLITE_BUSY. The single connection serializes access internally.
  try {
    sqlpp::sqlite3::connection connection(config);
    return std::make_shared<SqliteSingleCatalogStore>(options.catalog_name,
                                                      std::move(connection));
  } catch (const std::exception& e) {
    return IOError("Failed to open SQLite database '{}': {}", config->path_to_database,
                   e.what());
  }
}

}  // namespace iceberg::sql
