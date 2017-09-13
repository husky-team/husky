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


### GLOG ###

include (GNUInstallDirs)

if(GLOG_SEARCH_PATH)
    # Note: if using GLOG_SEARCH_PATH, the customized format is not activated.
    find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h PATHS ${GLOG_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    find_library(GLOG_LIBRARY NAMES glog PATHS ${GLOG_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    message(STATUS "Found GLog in search path ${GLOG_SEARCH_PATH}")
    message(STATUS "  (Headers)       ${GLOG_INCLUDE_DIR}")
    message(STATUS "  (Library)       ${GLOG_LIBRARY}")
else(GLOG_SEARCH_PATH)
    include(ExternalProject)
    set(THIRDPARTY_DIR ${PROJECT_BINARY_DIR}/third_party)
    set(GLOG_PATCH ${PROJECT_SOURCE_DIR}/third_party/patch/glog-customize-log-format-for-husky.patch)
    ExternalProject_Add(
        glog
        GIT_REPOSITORY "https://github.com/google/glog"
        GIT_TAG "v0.3.5"
        PREFIX ${THIRDPARTY_DIR}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
        CMAKE_ARGS -DWITH_GFLAGS=OFF
        CMAKE_ARGS -DBUILD_TESTING=OFF
        CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF
        UPDATE_COMMAND ""
        PATCH_COMMAND patch -p1 -t -R < ${GLOG_PATCH}
    )
    list(APPEND external_project_dependencies glog)
    set(GLOG_INCLUDE_DIR "${PROJECT_BINARY_DIR}/include")
    if(WIN32)
        set(GLOG_LIBRARY "${PROJECT_BINARY_DIR}/lib/libglog.lib")
    else(WIN32)
        set(GLOG_LIBRARY "${PROJECT_BINARY_DIR}/lib/libglog.a")
    endif(WIN32)
    message(STATUS "GLog will be built as a third party")
    message(STATUS "  (Headers should be)       ${GLOG_INCLUDE_DIR}")
    message(STATUS "  (Library should be)       ${GLOG_LIBRARY}")
endif(GLOG_SEARCH_PATH)
