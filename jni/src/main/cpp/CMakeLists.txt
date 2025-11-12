# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.24 FATAL_ERROR)
project(dingo-expr-jni C CXX)

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-std=c++17" COMPILER_SUPPORTS_CXX17)

set(LIB_EXPR_PATH ../../../../libexpr)

if(COMPILER_SUPPORTS_CXX17)
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED TRUE)
    add_compile_options(-fPIC)
else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++17 support. Please use a different C++ compiler.")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
cmake_policy(SET CMP0135 NEW)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

if(DEFINED ENV{JAVA_HOME})
    set(JAVA_HOME $ENV{JAVA_HOME})
    message(STATUS "ENV{JAVA_HOME} is set to ${JAVA_HOME}")
    include_directories(${JAVA_HOME}/include)
    if(APPLE)
        include_directories(${JAVA_HOME}/include/darwin)
    endif()
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        include_directories(${JAVA_HOME}/include/linux)
    endif()
    set(JNI_HEADER io_dingodb_expr_jni_LibExprJni.h)
    set(JNI_LIB_NAME expr_jni)
    add_custom_command(
        OUTPUT ${JNI_HEADER}
        COMMAND javac -h ./ ${CMAKE_SOURCE_DIR}/../java/io/dingodb/expr/jni/LibExprJni.java
    )
    add_library(${JNI_LIB_NAME} SHARED libexpr_jni.cc ${JNI_HEADER})
    target_include_directories(${JNI_LIB_NAME} PRIVATE ${LIB_EXPR_PATH}/build/contrib/gmp/install/include)
    target_include_directories(${JNI_LIB_NAME} PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
    target_include_directories(${JNI_LIB_NAME} PRIVATE ${LIB_EXPR_PATH}/src)
    target_include_directories(${JNI_LIB_NAME} PRIVATE ${LIB_EXPR_PATH}/src/types/decimal)
    target_link_directories(${JNI_LIB_NAME} PRIVATE ${LIB_EXPR_PATH}/build/src/expr)
    target_link_libraries(${JNI_LIB_NAME} PRIVATE expr)
else()
    message(FATAL_ERROR "ENV{JAVA_HOME} is not set.")
endif()
