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

#include "iceberg/cli/string_util.h"

#include <algorithm>
#include <cctype>

namespace iceberg::cli {

std::string Trim(std::string_view value) {
  auto begin = value.begin();
  auto end = value.end();
  while (begin != end && std::isspace(static_cast<unsigned char>(*begin))) {
    ++begin;
  }
  while (begin != end && std::isspace(static_cast<unsigned char>(*(end - 1)))) {
    --end;
  }
  return std::string(begin, end);
}

std::string Lower(std::string_view value) {
  std::string out(value);
  std::transform(out.begin(), out.end(), out.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return out;
}

std::vector<std::string> SplitStatements(std::string_view input) {
  std::vector<std::string> statements;
  std::string current;
  bool in_single_quote = false;
  for (char c : input) {
    if (c == '\'') {
      in_single_quote = !in_single_quote;
    }
    if (c == ';' && !in_single_quote) {
      auto statement = Trim(current);
      if (!statement.empty()) {
        statements.push_back(statement);
      }
      current.clear();
      continue;
    }
    current.push_back(c);
  }
  auto statement = Trim(current);
  if (!statement.empty()) {
    statements.push_back(statement);
  }
  return statements;
}

}  // namespace iceberg::cli
