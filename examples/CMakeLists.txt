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

if(LIBHDFS3_FOUND)
    include_directories(${LIBHDFS3_INCLUDE_DIR})
    add_definitions(-DWITH_HDFS)
endif(LIBHDFS3_FOUND)

if(MONGOCLIENT_FOUND)
    include_directories(${MONGOCLIENT_INCLUDE_DIR})
    add_definitions(-DWITH_MONGODB)
endif(MONGOCLIENT_FOUND)

set(husky husky-lib husky-io husky-core husky-base) 

### Applications

# WordCountMR
add_executable(WordCountMR wc_mr.cpp)
target_link_libraries(WordCountMR ${husky})
target_link_libraries(WordCountMR ${EXTERNAL_LIB})
set_property(TARGET WordCountMR PROPERTY CXX_STANDARD 14)

# WordCountMongo
add_executable(WordCountMongo wc_mr_mongo.cpp)
target_link_libraries(WordCountMongo ${husky})
target_link_libraries(WordCountMongo ${EXTERNAL_LIB})
set_property(TARGET WordCountMongo PROPERTY CXX_STANDARD 14)

# WordCountFlume
add_executable(WordCountFlume wc_mr_flume.cpp)
target_link_libraries(WordCountFlume ${husky})
target_link_libraries(WordCountFlume ${EXTERNAL_LIB})
set_property(TARGET WordCountFlume PROPERTY CXX_STANDARD 14)

# PI
add_executable(PI pi.cpp)
target_link_libraries(PI ${husky})
target_link_libraries(PI ${EXTERNAL_LIB})
set_property(TARGET PI PROPERTY CXX_STANDARD 14)

# CC
add_executable(CC connected_components.cpp)
target_link_libraries(CC ${husky})
target_link_libraries(CC ${EXTERNAL_LIB})
set_property(TARGET CC PROPERTY CXX_STANDARD 14)

# PageRank
add_executable(PageRank pagerank.cpp)
target_link_libraries(PageRank ${husky})
target_link_libraries(PageRank ${EXTERNAL_LIB})
set_property(TARGET PageRank PROPERTY CXX_STANDARD 14)

# Aggregator
add_executable(Aggregator aggregator.cpp)
target_link_libraries(Aggregator ${husky})
target_link_libraries(Aggregator ${EXTERNAL_LIB})
set_property(TARGET Aggregator PROPERTY CXX_STANDARD 14)

# PR_CC
add_executable(PR_CC pr_cc_attrlist.cpp)
target_link_libraries(PR_CC ${husky})
target_link_libraries(PR_CC ${EXTERNAL_LIB})
set_property(TARGET PR_CC PROPERTY CXX_STANDARD 14)
