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


include_directories(${PROJECT_SOURCE_DIR} ${Boost_INCLUDE_DIRS})

set(master_plugins mongodb_assigner.cpp nfs_assigner.cpp progress_tracker.cpp nfs_binary_assigner.cpp)

if(LIBHDFS3_FOUND)
    include_directories(${LIBHDFS3_INCLUDE_DIR})
    add_definitions(-DWITH_HDFS)
    set(master_plugins ${master_plugins} hdfs_assigner.cpp hdfs_binary_assigner.cpp)
endif(LIBHDFS3_FOUND)

if(MONGOCLIENT_FOUND)
    include_directories(${MONGOCLIENT_INCLUDE_DIR})
    add_definitions(-DWITH_MONGODB)
endif(MONGOCLIENT_FOUND)

set(husky husky-lib husky-io husky-core husky-base)

# Master
add_executable(Master master.cpp ${master_plugins})
target_link_libraries(Master ${husky})
target_link_libraries(Master ${EXTERNAL_LIB})
set_property(TARGET Master PROPERTY CXX_STANDARD 14)
