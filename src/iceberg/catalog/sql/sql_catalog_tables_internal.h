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

/// \file iceberg/catalog/sql/sql_catalog_tables_internal.h
/// sqlpp23 table models for the SQL catalog's backing tables.
///
/// These mirror the hand-written shape that sqlpp23's `ddl2cpp` generator would
/// produce. The schema is compatible with the Apache Iceberg Java
/// `JdbcCatalog`: `iceberg_tables` and `iceberg_namespace_properties`, both
/// scoped by a `catalog_name` column.
///
/// This is an internal header: it pulls in sqlpp23 and is only included by the
/// built-in store implementations, never by public catalog headers.

#include <optional>

#include <sqlpp23/core/basic/table.h>
#include <sqlpp23/core/basic/table_columns.h>
#include <sqlpp23/core/name/create_name_tag.h>
#include <sqlpp23/core/type_traits.h>

namespace iceberg::sql {

// sqlpp23 name tags require a static `const char name[]` member; the
// `SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP` macro provides that required shape.
// NOLINTBEGIN(modernize-avoid-c-arrays)

struct IcebergTables_ {
  struct CatalogName {
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(catalog_name, catalogName);
    using data_type = ::sqlpp::text;
    using has_default = std::false_type;
  };
  struct TableNamespace {
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(table_namespace, tableNamespace);
    using data_type = ::sqlpp::text;
    using has_default = std::false_type;
  };
  struct TableName {
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(table_name, tableName);
    using data_type = ::sqlpp::text;
    using has_default = std::false_type;
  };
  struct MetadataLocation {
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(metadata_location, metadataLocation);
    using data_type = std::optional<::sqlpp::text>;
    using has_default = std::true_type;
  };
  struct PreviousMetadataLocation {
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(previous_metadata_location,
                                          previousMetadataLocation);
    using data_type = std::optional<::sqlpp::text>;
    using has_default = std::true_type;
  };
  struct RecordType {
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(iceberg_type, recordType);
    using data_type = std::optional<::sqlpp::text>;
    using has_default = std::true_type;
  };

  SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(iceberg_tables, icebergTables);

  template <typename T>
  using _table_columns = sqlpp::table_columns<T,                         //
                                              CatalogName,               //
                                              TableNamespace,            //
                                              TableName,                 //
                                              MetadataLocation,          //
                                              PreviousMetadataLocation,  //
                                              RecordType>;
  using _required_insert_columns = sqlpp::detail::type_set<
      sqlpp::column_t<sqlpp::table_t<IcebergTables_>, CatalogName>,
      sqlpp::column_t<sqlpp::table_t<IcebergTables_>, TableNamespace>,
      sqlpp::column_t<sqlpp::table_t<IcebergTables_>, TableName>>;
};
using IcebergTables = ::sqlpp::table_t<IcebergTables_>;

struct IcebergNamespaceProperties_ {
  struct CatalogName {
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(catalog_name, catalogName);
    using data_type = ::sqlpp::text;
    using has_default = std::false_type;
  };
  struct Namespace {
    // SQL column name is `namespace`; the C++ member cannot be a keyword.
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(namespace, ns);
    using data_type = ::sqlpp::text;
    using has_default = std::false_type;
  };
  struct PropertyKey {
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(property_key, propertyKey);
    using data_type = ::sqlpp::text;
    using has_default = std::false_type;
  };
  struct PropertyValue {
    SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(property_value, propertyValue);
    using data_type = std::optional<::sqlpp::text>;
    using has_default = std::true_type;
  };

  SQLPP_CREATE_NAME_TAG_FOR_SQL_AND_CPP(iceberg_namespace_properties,
                                        icebergNamespaceProperties);

  template <typename T>
  using _table_columns = sqlpp::table_columns<T,            //
                                              CatalogName,  //
                                              Namespace,    //
                                              PropertyKey,  //
                                              PropertyValue>;
  using _required_insert_columns = sqlpp::detail::type_set<
      sqlpp::column_t<sqlpp::table_t<IcebergNamespaceProperties_>, CatalogName>,
      sqlpp::column_t<sqlpp::table_t<IcebergNamespaceProperties_>, Namespace>,
      sqlpp::column_t<sqlpp::table_t<IcebergNamespaceProperties_>, PropertyKey>>;
};
using IcebergNamespaceProperties = ::sqlpp::table_t<IcebergNamespaceProperties_>;

// NOLINTEND(modernize-avoid-c-arrays)

}  // namespace iceberg::sql
