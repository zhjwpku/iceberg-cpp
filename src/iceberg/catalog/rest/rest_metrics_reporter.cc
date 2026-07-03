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

#include "iceberg/catalog/rest/rest_metrics_reporter.h"

#include <memory>
#include <utility>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/rest_catalog.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

RestMetricsReporter::RestMetricsReporter(const RestCatalog& catalog,
                                         TableIdentifier identifier,
                                         std::shared_ptr<auth::AuthSession> session)
    : catalog_(&catalog),
      identifier_(std::move(identifier)),
      session_(std::move(session)) {}

Status RestMetricsReporter::Report(const MetricsReport& report) {
  ICEBERG_DCHECK(catalog_ != nullptr, "REST metrics reporter catalog must not be null");
  ICEBERG_DCHECK(session_ != nullptr, "REST metrics reporter session must not be null");

  // Metrics reporting follows Java's best-effort behavior: failures are ignored.
  (void)catalog_->ReportMetrics(identifier_, report, *session_);
  return {};
}

}  // namespace iceberg::rest
