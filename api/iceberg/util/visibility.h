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

#if defined(_WIN32) || defined(__CYGWIN__)
// Windows

#  if defined(_MSC_VER)
#    pragma warning(disable : 4251)
#  else
#    pragma GCC diagnostic ignored "-Wattributes"
#  endif

#  if defined(__cplusplus) && defined(__GNUC__) && !defined(__clang__)
// Use C++ attribute syntax where possible to avoid GCC parser bug
// (https://stackoverflow.com/questions/57993818/gcc-how-to-combine-attribute-dllexport-and-nodiscard-in-a-struct-de)
#    define ICEBERG_DLLEXPORT [[gnu::dllexport]]
#    define ICEBERG_DLLIMPORT [[gnu::dllimport]]
#  else
#    define ICEBERG_DLLEXPORT __declspec(dllexport)
#    define ICEBERG_DLLIMPORT __declspec(dllimport)
#  endif

// _declspec(dllexport) even when the #included by a non-iceberg source
#  define ICEBERG_FORCE_EXPORT ICEBERG_DLLEXPORT

#  ifdef ICEBERG_STATIC
#    define ICEBERG_EXPORT
#    define ICEBERG_FRIEND_EXPORT
#    define ICEBERG_TEMPLATE_EXPORT
#  elif defined(ICEBERG_EXPORTING)
#    define ICEBERG_EXPORT ICEBERG_DLLEXPORT
// For some reason [[gnu::dllexport]] doesn't work well with friend declarations
#    define ICEBERG_FRIEND_EXPORT __declspec(dllexport)
#    define ICEBERG_TEMPLATE_EXPORT ICEBERG_DLLEXPORT
#  else
#    define ICEBERG_EXPORT ICEBERG_DLLIMPORT
#    define ICEBERG_FRIEND_EXPORT __declspec(dllimport)
#    define ICEBERG_TEMPLATE_EXPORT ICEBERG_DLLIMPORT
#  endif

#  define ICEBERG_NO_EXPORT

#else

// Non-Windows

#  if defined(__cplusplus) && (defined(__GNUC__) || defined(__clang__))
#    ifndef ICEBERG_EXPORT
#      define ICEBERG_EXPORT [[gnu::visibility("default")]]
#    endif
#    ifndef ICEBERG_NO_EXPORT
#      define ICEBERG_NO_EXPORT [[gnu::visibility("hidden")]]
#    endif
#  else
// Not C++, or not gcc/clang
#    ifndef ICEBERG_EXPORT
#      define ICEBERG_EXPORT
#    endif
#    ifndef ICEBERG_NO_EXPORT
#      define ICEBERG_NO_EXPORT
#    endif
#  endif

#  define ICEBERG_FRIEND_EXPORT
#  define ICEBERG_TEMPLATE_EXPORT

// [[gnu::visibility("default")]] even when #included by a non-iceberg source
#  define ICEBERG_FORCE_EXPORT [[gnu::visibility("default")]]

#endif  // Non-Windows
