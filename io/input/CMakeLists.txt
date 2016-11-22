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

set(input_cpps inputformat_helper.cpp
    line_inputformat.cpp
    chunk_inputformat.cpp
    separator_inputformat.cpp
    xml_inputformat.cpp
    nfs_file_splitter.cpp
    nfs_binary_inputformat.cpp
    binary_inputformat_impl.cpp
    binary_inputformat.cpp)

if(LIBHDFS3_FOUND)
    include_directories(${LIBHDFS3_INCLUDE_DIR})
    add_definitions(-DWITH_HDFS)
    set(input_cpps ${input_cpps} hdfs_file_splitter.cpp hdfs_binary_inputformat.cpp)
endif(LIBHDFS3_FOUND)

if(MONGOCLIENT_FOUND)
    include_directories(${MONGOCLIENT_INCLUDE_DIR})
    add_definitions(-DWITH_MONGODB)
    set(input_cpps ${input_cpps} mongodb_split.cpp mongodb_inputformat.cpp)
endif(MONGOCLIENT_FOUND)

if(THRIFT_FOUND)
    include_directories(${THRIFT_INCLUDE_DIR})
    add_definitions(-DWITH_THRIFT)
    add_subdirectory(flume_connector)
    set(input_cpps ${input_cpps} flume_connector/flume_types.cpp flume_connector/flume_constants.cpp flume_connector/ThriftSourceProtocol.cpp flume_inputformat.cpp)
endif(THRIFT_FOUND)

set(input_cpps ${input_cpps} inputformat_store.cpp)

add_library(input-objs OBJECT ${input_cpps})
set_property(TARGET input-objs PROPERTY CXX_STANDARD 14)
