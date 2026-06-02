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

import os

import lit.formats

config.name = "iceberg-cli-regression"
config.test_format = lit.formats.ShTest(True)
config.suffixes = [".sql", ".isolation"]

config.test_source_root = os.path.dirname(__file__)
config.test_exec_root = lit_config.params.get(
    "test_exec_root", os.path.join(config.test_source_root, "Output")
)

iceberg_cli = lit_config.params.get("iceberg_cli")
if not iceberg_cli:
    lit_config.fatal("missing required lit parameter: iceberg_cli")

cmake = lit_config.params.get("cmake", "cmake")

config.substitutions.append(("%iceberg_cli", iceberg_cli))
config.substitutions.append(("%cmake", cmake))
config.substitutions.append(
    (
        "%run_cli_sql_case",
        f'"{cmake}" -DCLI_EXECUTABLE="{iceberg_cli}"',
    )
)
config.substitutions.append(
    (
        "%run_cli_isolation_case",
        f'"{cmake}" -DCLI_EXECUTABLE="{iceberg_cli}"',
    )
)
