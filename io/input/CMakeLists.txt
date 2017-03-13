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


include_directories(${PROJECT_SOURCE_DIR} ${EXTERNAL_INCLUDE})

file(GLOB io-input-src-files
    input_cpps
    inputformat_helper.cpp
    line_inputformat.cpp
    chunk_inputformat.cpp
    separator_inputformat.cpp
    xml_inputformat.cpp
    nfs_file_splitter.cpp
    nfs_binary_inputformat.cpp
    binary_inputformat_impl.cpp
    binary_inputformat.cpp
    inputformat_store.cpp)

if(LIBHDFS3_FOUND)
    file(GLOB io-input-hdfs-src-files hdfs_file_splitter.cpp hdfs_binary_inputformat.cpp)
    list(APPEND io-input-src-files ${io-input-hdfs-src-files})
endif(LIBHDFS3_FOUND)

if(MONGOCLIENT_FOUND)
    file(GLOB io-input-mongo-src-files mongodb_split.cpp mongodb_inputformat.cpp)
    list(APPEND io-input-src-files ${io-input-mongo-src-files})
endif(MONGOCLIENT_FOUND)

if(THRIFT_FOUND)
    file(GLOB io-input-flume-src-files
        flume_connector/flume_types.cpp
        flume_connector/flume_constants.cpp
        flume_connector/ThriftSourceProtocol.cpp
        flume_inputformat.cpp)
    list(APPEND io-input-src-files ${io-input-flume-src-files})
endif(THRIFT_FOUND)

if(ORC_FOUND)
    file(GLOB io-input-orc-src-files orc_file_splitter.cpp orc_inputformat.cpp)
    list(APPEND io-input-src-files ${io-input-orc-src-files})
endif(ORC_FOUND)

husky_cache_variable(io-input-src-files ${io-input-src-files})

add_library(input-objs OBJECT ${io-input-src-files})
husky_default_properties(input-objs)
