#!/usr/bin/env bash
#
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

set -eux

source_dir=${1}
build_dir=${1}/build

mkdir ${build_dir}
pushd ${build_dir}

CMAKE_ARGS=(
    "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX:-${ICEBERG_HOME}}"
    "-DICEBERG_BUILD_STATIC=ON"
    "-DICEBERG_BUILD_SHARED=ON"
    "-DCMAKE_BUILD_TYPE=Debug"
)

BUILD_ARGS=()

# Add Windows-specific toolchain file if on Windows
if [[ "${OSTYPE}" == "msys" || "${OSTYPE}" == "win32" ]]; then
    CMAKE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
    BUILD_ARGS+=("--config Debug")
fi

cmake "${CMAKE_ARGS[@]}" ${source_dir}
cmake --build . "${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}" --target install
ctest --output-on-failure -C Debug

popd

# clean up between builds
rm -rf ${build_dir}
