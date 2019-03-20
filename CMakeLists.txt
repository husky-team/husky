# Copyright 2016 Husky Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


project(husky)
cmake_minimum_required(VERSION 3.0.2)

# CMake Options
set(CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
if(CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS_DEBUG "-g -DHUSKY_DEBUG_MODE")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DHUSKY_DEBUG_MODE")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -w")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
endif()

option(BUILD_SHARED_LIBRARY "Build shared library" OFF)

# Boost Libraries
find_package(Threads)
find_package(Boost 1.58.0 COMPONENTS system filesystem regex thread program_options date_time chrono)

# CMake Utilities
include(cmake/utils.cmake)

# External Dependencies
include(cmake/modules/glog.cmake)
include(cmake/modules/gtest.cmake)
include(cmake/modules/zeromq.cmake)
include(cmake/dep.cmake)

# External Includes
set(HUSKY_EXTERNAL_INCLUDE ${Boost_INCLUDE_DIRS} ${TCMALLOC_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR} ${GLOG_INCLUDE_DIR} ${EIGEN_INCLUDE_DIR})

# External Libraries
set(HUSKY_EXTERNAL_LIB ${GLOG_LIBRARY} ${ZMQ_LIBRARY} ${TCMALLOC_LIBRARY} ${CMAKE_THREAD_LIBS_INIT} ${Boost_LIBRARIES})

# External Definitions
set(HUSKY_EXTERNAL_DEFINITION "")

# libhdfs3
if(LIBHDFS3_FOUND)
    list(APPEND HUSKY_EXTERNAL_INCLUDE ${LIBHDFS3_INCLUDE_DIR})
    list(APPEND HUSKY_EXTERNAL_LIB ${LIBHDFS3_LIBRARY})
    list(APPEND HUSKY_EXTERNAL_DEFINITION ${LIBHDFS3_DEFINITION})
endif(LIBHDFS3_FOUND)

# MongoDB
if(MONGOCLIENT_FOUND)
    list(APPEND HUSKY_EXTERNAL_INCLUDE ${MONGOCLIENT_INCLUDE_DIR})
    list(APPEND HUSKY_EXTERNAL_LIB ${RT_LIBRARY} ${MONGOCLIENT_LIBRARY})
    list(APPEND HUSKY_EXTERNAL_DEFINITION ${MONGOCLIENT_DEFINITION})
endif(MONGOCLIENT_FOUND)

# Thrift
if(THRIFT_FOUND)
    list(APPEND HUSKY_EXTERNAL_INCLUDE ${THRIFT_INCLUDE_DIR})
    list(APPEND HUSKY_EXTERNAL_LIB ${THRIFT_LIBRARY})
    list(APPEND HUSKY_EXTERNAL_DEFINITION ${THRIFT_DEFINITION})
endif(THRIFT_FOUND)

if(WIN32)
    list(APPEND HUSKY_EXTERNAL_LIB wsock32 ws2_32)
endif()

husky_cache_variable(HUSKY_EXTERNAL_INCLUDE ${HUSKY_EXTERNAL_INCLUDE})
husky_cache_variable(HUSKY_EXTERNAL_LIB ${HUSKY_EXTERNAL_LIB})
husky_cache_variable(HUSKY_EXTERNAL_DEFINITION ${HUSKY_EXTERNAL_DEFINITION})

add_definitions(${HUSKY_EXTERNAL_DEFINITION})

add_subdirectory(base)
add_subdirectory(core)
add_subdirectory(io)
add_subdirectory(lib)
add_subdirectory(examples)
add_subdirectory(master)
add_subdirectory(test)
add_subdirectory(benchmarks)

# libhusky.a
set(husky-objs $<TARGET_OBJECTS:channel-objs> $<TARGET_OBJECTS:base-objs> $<TARGET_OBJECTS:core-objs> ${io-objs} ${lib-objs})
add_library(husky STATIC ${husky-objs})

if(BUILD_SHARED_LIBRARY)
    include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})
    set(husky-src-files ${base-src-files} ${core-src-files} ${core-channel-src-files}
        ${io-src-files} ${io-input-src-files} ${io-output-src-files} ${lib-src-files})
    add_library(husky-shared SHARED ${husky-src-files})
endif(BUILD_SHARED_LIBRARY)
