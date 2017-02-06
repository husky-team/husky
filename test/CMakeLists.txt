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


include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

set(husky husky-lib husky-io husky-core husky-base) 

# Unit Test

file(GLOB_RECURSE UNITTEST_FILES ${PROJECT_SOURCE_DIR}/*_unittest.cpp) 
remove_definitions(-DWITH_THRIFT)  # Workaround for namespace collisions between gtest and flume.
include_directories(${GTEST_INCLUDE_DIR})
add_executable(HuskyUnitTest ${UNITTEST_FILES} unittest_main.cpp)
add_dependencies(HuskyUnitTest gtest)
target_link_libraries(HuskyUnitTest ${husky})
target_link_libraries(HuskyUnitTest ${HUSKY_EXTERNAL_LIB})
target_link_libraries(HuskyUnitTest ${GTEST_LIBRARIES})
target_link_libraries(HuskyUnitTest ${GMOCK_LIBRARIES})
husky_default_properties(HuskyUnitTest)

# Functional Test

# TestAsync 
add_executable(TestAsync test-async.cpp)
target_link_libraries(TestAsync ${husky})
target_link_libraries(TestAsync ${HUSKY_EXTERNAL_LIB})
husky_default_properties(TestAsync)

# TestBinaryInputFormat
add_executable(TestBinaryInputFormat test-binary-inputformat.cpp)
target_link_libraries(TestBinaryInputFormat ${husky})
target_link_libraries(TestBinaryInputFormat ${HUSKY_EXTERNAL_LIB})
husky_default_properties(TestBinaryInputFormat)

# TestLogMsg
add_executable(TestLogMsg test-log-msg.cpp)
target_link_libraries(TestLogMsg ${husky})
target_link_libraries(TestLogMsg ${HUSKY_EXTERNAL_LIB})
husky_default_properties(TestLogMsg)

# TestMongoDBOutputFormat
add_executable(TestMongoDBOutputFormat test-mongodb-outputformat.cpp)
target_link_libraries(TestMongoDBOutputFormat ${husky})
target_link_libraries(TestMongoDBOutputFormat ${HUSKY_EXTERNAL_LIB})
husky_default_properties(TestMongoDBOutputFormat)
