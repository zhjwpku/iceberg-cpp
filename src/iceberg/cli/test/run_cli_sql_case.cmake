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

if(NOT DEFINED CLI_EXECUTABLE)
  message(FATAL_ERROR "CLI_EXECUTABLE is required")
endif()
if(NOT DEFINED SQL_FILE)
  message(FATAL_ERROR "SQL_FILE is required")
endif()
if(NOT DEFINED EXPECTED_FILE)
  message(FATAL_ERROR "EXPECTED_FILE is required")
endif()
if(NOT DEFINED CASE_NAME)
  message(FATAL_ERROR "CASE_NAME is required")
endif()
if(NOT DEFINED WORK_DIR)
  message(FATAL_ERROR "WORK_DIR is required")
endif()

set(WAREHOUSE_DIR "${WORK_DIR}/warehouse")
file(REMOVE_RECURSE "${WORK_DIR}")
file(MAKE_DIRECTORY "${WORK_DIR}")

execute_process(COMMAND "${CLI_EXECUTABLE}" --warehouse "${WAREHOUSE_DIR}" --file
                        "${SQL_FILE}"
                RESULT_VARIABLE cli_result
                OUTPUT_VARIABLE actual_output
                ERROR_VARIABLE actual_error)

if(NOT cli_result EQUAL 0)
  message(FATAL_ERROR "CLI SQL test '${CASE_NAME}' failed with exit code ${cli_result}\n${actual_error}"
  )
endif()

string(REPLACE "\r\n" "\n" actual_output "${actual_output}")
string(REGEX
       REPLACE "file=[^ \n\t]*/([^/ \n\t]+)/data/insert-[0-9a-fA-F-]+\\.parquet"
               "file=<warehouse>/\\1/data/insert-<uuid>.parquet" actual_output
               "${actual_output}")

file(READ "${EXPECTED_FILE}" expected_output)
string(REPLACE "\r\n" "\n" expected_output "${expected_output}")

file(WRITE "${WORK_DIR}/actual.out" "${actual_output}")
file(WRITE "${WORK_DIR}/expected.out" "${expected_output}")

if(NOT actual_output STREQUAL expected_output)
  message(FATAL_ERROR "CLI SQL test '${CASE_NAME}' output did not match expected output.\n"
                      "SQL: ${SQL_FILE}\n"
                      "Expected: ${WORK_DIR}/expected.out\n"
                      "Actual: ${WORK_DIR}/actual.out")
endif()
