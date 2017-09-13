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


### ZeroMQ ###

# TODO(legend): check zeromq version?
find_path(ZMQ_INCLUDE_DIR NAMES zmq.hpp PATHS ${ZMQ_SEARCH_PATH})
find_library(ZMQ_LIBRARY NAMES zmq PATHS ${ZMQ_SEARCH_PATH})

if(ZMQ_INCLUDE_DIR AND ZMQ_LIBRARY)
    set(ZMQ_FOUND true)
endif(ZMQ_INCLUDE_DIR AND ZMQ_LIBRARY)

if(ZMQ_FOUND)
    message (STATUS "Found ZeroMQ:")
    message (STATUS "  (Headers)       ${ZMQ_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${ZMQ_LIBRARY}")
else(ZMQ_FOUND)
    message (STATUS "ZeroMQ will be included as a third party:")
    include(ExternalProject)
    set(THIRDPARTY_DIR ${PROJECT_BINARY_DIR}/third_party)
    if(NOT ZMQ_INCLUDE_DIR)
        ExternalProject_Add(
            cppzmq
            GIT_REPOSITORY "https://github.com/zeromq/cppzmq"
            GIT_TAG "v4.2.2"
            PREFIX ${THIRDPARTY_DIR}
            UPDATE_COMMAND ""
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            INSTALL_COMMAND cp ${THIRDPARTY_DIR}/src/cppzmq/zmq.hpp ${PROJECT_BINARY_DIR}/include/zmq.hpp
        )
        list(APPEND external_project_dependencies cppzmq)
        add_dependencies(cppzmq libzmq)
    endif(NOT ZMQ_INCLUDE_DIR)
    if(NOT ZMQ_LIBRARY)
        ExternalProject_Add(
            libzmq
            GIT_REPOSITORY "https://github.com/zeromq/libzmq"
            GIT_TAG "v4.2.2"
            PREFIX ${THIRDPARTY_DIR}
            UPDATE_COMMAND ""
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
            CMAKE_ARGS -DZMQ_BUILD_TESTS=OFF
        )
        list(APPEND external_project_dependencies libzmq)
    endif(NOT ZMQ_LIBRARY)
    set(ZMQ_INCLUDE_DIR "${PROJECT_BINARY_DIR}/include")
    if(WIN32)
        set(ZMQ_LIBRARY "${PROJECT_BINARY_DIR}/lib/libzmq.lib")
    else(WIN32)
        set(ZMQ_LIBRARY "${PROJECT_BINARY_DIR}/lib/libzmq.so")
    endif(WIN32)
    message (STATUS "  (Headers should be)       ${ZMQ_INCLUDE_DIR}")
    message (STATUS "  (Library should be)       ${ZMQ_LIBRARY}")
    set(ZEROMQ_FOUND true)
endif(ZMQ_FOUND)
