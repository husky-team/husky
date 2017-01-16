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


include(ExternalProject)

### GLOG ###

set(GLOG_PATCH ${PROJECT_SOURCE_DIR}/third_party/patch/glog-customize-log-format-for-husky.patch)

if(WIN32)
ExternalProject_Add(
    glog
    GIT_REPOSITORY "https://github.com/google/glog"
    PREFIX ${PROJECT_BINARY_DIR}/glog
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
    CMAKE_ARGS -DWITH_GFLAGS=OFF
    CMAKE_ARGS -DBUILD_TESTING=OFF
    UPDATE_COMMAND ""
    PATCH_COMMAND ""
)
else(WIN32)
ExternalProject_Add(
    glog
    GIT_REPOSITORY "https://github.com/google/glog"
    PREFIX ${PROJECT_BINARY_DIR}/glog
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
    CMAKE_ARGS -DWITH_GFLAGS=OFF
    CMAKE_ARGS -DBUILD_TESTING=OFF
    UPDATE_COMMAND ""
    PATCH_COMMAND patch -p1 -t -R < ${GLOG_PATCH}
)
endif(WIN32)


set(GLOG_INCLUDE "${PROJECT_BINARY_DIR}/include")
if(WIN32)
set(GLOG_LIBRARY "${PROJECT_BINARY_DIR}/lib/glog.lib")
else(WIN32)
set(GLOG_LIBRARY "${PROJECT_BINARY_DIR}/lib/libglog.a")
endif(WIN32)
list(APPEND external_project_dependencies glog)

### GTest, GMock ###

# No need to append gtest to external_project_dependencies;
# No need to append to EXTERNAL_INCLUDE and EXTERNAL_LIB;
# Since it is only needed by target HuskyUnitTest.
find_package(Threads REQUIRED)
include(ExternalProject)
ExternalProject_Add(
    gtest
    GIT_REPOSITORY https://github.com/google/googletest.git
    PREFIX ${PROJECT_BINARY_DIR}/gtest
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
    CMAKE_ARGS -Dgtest_disable_pthreads=ON
    UPDATE_COMMAND ""
)
set(GTEST_INCLUDE "${PROJECT_BINARY_DIR}/include")
if(WIN32)
set(GTEST_LIBRARIES
    "${PROJECT_BINARY_DIR}/lib/gtest.lib"
    "${PROJECT_BINARY_DIR}/lib/gtest_main.lib")

set(GMOCK_LIBRARIES
    "${PROJECT_BINARY_DIR}/lib/gmock.lib"
    "${PROJECT_BINARY_DIR}/lib/gmock_main.lib")
else(WIN32)
set(GTEST_LIBRARIES
    "${PROJECT_BINARY_DIR}/lib/libgtest.a"
    "${PROJECT_BINARY_DIR}/lib/libgtest_main.a")

set(GMOCK_LIBRARIES
    "${PROJECT_BINARY_DIR}/lib/libgmock.a"
    "${PROJECT_BINARY_DIR}/lib/libgmock_main.a")
endif(WIN32)
