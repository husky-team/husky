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


include_directories(${PROJECT_SOURCE_DIR})

set(husky husky-lib husky-io husky-core husky-base) 

# GTest, GMock

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
include_directories(${GTEST_INCLUDE})

set(GTEST_LIBRARIES
    "${PROJECT_BINARY_DIR}/lib/libgtest.a"
    "${PROJECT_BINARY_DIR}/lib/libgtest_main.a")

set(GMOCK_LIBRARIES
    "${PROJECT_BINARY_DIR}/lib/libgmock.a"
    "${PROJECT_BINARY_DIR}/lib/libgmock_main.a")

# Unit Test

file(GLOB_RECURSE UNITTEST_FILES ${PROJECT_SOURCE_DIR}/*_unittest.cpp) 
add_executable(HuskyUnitTest ${UNITTEST_FILES} unittest_main.cpp)
add_dependencies(HuskyUnitTest gtest)
target_link_libraries(HuskyUnitTest ${husky})
target_link_libraries(HuskyUnitTest ${EXTERNAL_LIB})
target_link_libraries(HuskyUnitTest ${GTEST_LIBRARIES})
target_link_libraries(HuskyUnitTest ${GMOCK_LIBRARIES})
set_property(TARGET HuskyUnitTest PROPERTY CXX_STANDARD 14)

# TestAsync 
add_executable(TestAsync test-async.cpp)
target_link_libraries(TestAsync ${husky})
target_link_libraries(TestAsync ${EXTERNAL_LIB})
set_property(TARGET TestAsync PROPERTY CXX_STANDARD 14)
