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


### GTest, GMock ###

# No need to append gtest to external_project_dependencies;
# No need to append to EXTERNAL_INCLUDE and EXTERNAL_LIB;
# Since it is only needed by target HuskyUnitTest.

if(GTEST_SEARCH_PATH)
    find_path(GTEST_INCLUDE_DIR NAMES gtest/gtest.h PATHS ${GTEST_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    find_library(GTEST_LIBRARY NAMES gtest PATHS ${GTEST_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    message(STATUS "Found GTest in search path ${GTEST_SEARCH_PATH}")
    message(STATUS "  (Headers)       ${GTEST_INCLUDE_DIR}")
    message(STATUS "  (Library)       ${GTEST_LIBRARY}")
else(GTEST_SEARCH_PATH)
    find_package(Threads REQUIRED)
    include(ExternalProject)

    set(THIRDPARTY_DIR ${PROJECT_BINARY_DIR}/third_party)
    ExternalProject_Add(
        gtest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG "release-1.8.0"
        PREFIX ${THIRDPARTY_DIR}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
        CMAKE_ARGS -Dgtest_disable_pthreads=ON
        UPDATE_COMMAND ""
    )
    set(GTEST_INCLUDE_DIR "${PROJECT_BINARY_DIR}/include")
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
    message(STATUS "GTest will be built as a third party")
    message(STATUS "  (Headers should be)       ${GTEST_INCLUDE_DIR}")
    message(STATUS "  (Library should be)       ${GTEST_LIBRARIES}")
endif(GTEST_SEARCH_PATH)
