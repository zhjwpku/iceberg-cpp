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

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"

/// \file iceberg/catalog/rest/rest_metrics_reporter.h
/// REST metrics reporter for the Iceberg REST Catalog API.

namespace iceberg::rest {

/// \brief Metrics reporter that sends reports to a table's REST metrics endpoint.
class ICEBERG_REST_EXPORT RestMetricsReporter final : public MetricsReporter {
 public:
  RestMetricsReporter(const RestCatalog& catalog, TableIdentifier identifier,
                      std::shared_ptr<auth::AuthSession> session);

  Status Report(const MetricsReport& report) override;

 private:
  const RestCatalog* catalog_;
  TableIdentifier identifier_;
  std::shared_ptr<auth::AuthSession> session_;
};

}  // namespace iceberg::rest
