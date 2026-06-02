# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

add_executable(iceberg_cli
               cli_app.cc
               iceberg_cli.cc
               isolation_runner.cc
               runner.cc
               sql_parser.cc
               string_util.cc)
target_include_directories(iceberg_cli PRIVATE "${PROJECT_BINARY_DIR}/src"
                                               "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(iceberg_cli
                      PRIVATE "$<IF:$<TARGET_EXISTS:iceberg_bundle_static>,iceberg_bundle_static,iceberg_bundle_shared>"
                              "$<IF:$<TARGET_EXISTS:iceberg_data_static>,iceberg_data_static,iceberg_data_shared>"
                              "$<IF:$<TARGET_EXISTS:iceberg_static>,iceberg_static,iceberg_shared>"
                              "$<IF:$<TARGET_EXISTS:Arrow::arrow_static>,Arrow::arrow_static,Arrow::arrow_shared>"
                              "$<IF:$<TARGET_EXISTS:Parquet::parquet_static>,Parquet::parquet_static,Parquet::parquet_shared>"
                              linenoise_static
                              pg_query_static
                              nlohmann_json::nlohmann_json)

install(TARGETS iceberg_cli RUNTIME DESTINATION "${ICEBERG_INSTALL_BINDIR}")

if(ICEBERG_BUILD_TESTS)
  find_program(ICEBERG_LLVM_LIT NAMES llvm-lit lit)
  if(ICEBERG_LLVM_LIT)
    add_test(NAME iceberg_cli_regression
             COMMAND "${ICEBERG_LLVM_LIT}" --param
                     "iceberg_cli=$<TARGET_FILE:iceberg_cli>" --param
                     "cmake=${CMAKE_COMMAND}" --param
                     "test_exec_root=${CMAKE_CURRENT_BINARY_DIR}/test/lit"
                     "${CMAKE_CURRENT_SOURCE_DIR}/test")
    set_tests_properties(iceberg_cli_regression PROPERTIES LABELS
                                                           "iceberg_cli;regression")
  else()
    message(STATUS "llvm-lit/lit not found; skipping iceberg_cli lit tests")
  endif()
endif()
