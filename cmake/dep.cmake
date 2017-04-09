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


### Eigen ###

# TODO: Check the version of Eigen
set(EIGEN_FIND_REQUIRED true)
find_path(EIGEN_INCLUDE_DIR NAMES eigen3/Eigen/Dense)
set(EIGEN_INCLUDE_DIR ${EIGEN_INCLUDE_DIR}/eigen3)
if(EIGEN_INCLUDE_DIR)
    set(EIGEN_FOUND true)
endif(EIGEN_INCLUDE_DIR)
if(EIGEN_FOUND)
    set(DEP_FOUND true)
    if(NOT EIGEN_FIND_QUIETLY)
        message (STATUS "Found Eigen:")
        message (STATUS "  (Headers)       ${EIGEN_INCLUDE_DIR}")
    endif(NOT EIGEN_FIND_QUIETLY)
else(EIGEN_FOUND)
    set(DEP_FOUND false)
    if(EIGEN_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find Eigen")
    endif(EIGEN_FIND_REQUIRED)
endif(EIGEN_FOUND)

### TCMalloc ###

set(TCMALLOC_FIND_REQUIRED true)
find_path(TCMALLOC_INCLUDE_DIR NAMES gperftools/malloc_extension.h)
find_library(TCMALLOC_LIBRARY NAMES tcmalloc)
if(TCMALLOC_LIBRARY)
    set(TCMALLOC_FOUND true)
endif(TCMALLOC_LIBRARY)
if(TCMALLOC_FOUND)
    set(DEP_FOUND true)
    if(NOT TCMALLOC_FIND_QUIETLY)
        message (STATUS "Found TCMalloc:")
        message (STATUS "  (Library)       ${TCMALLOC_LIBRARY}")
    endif(NOT TCMALLOC_FIND_QUIETLY)
else(TCMALLOC_FOUND)
    set(DEP_FOUND false)
    if(TCMALLOC_FIND_REQUIRED)
        message(FATAL_ERROR "Could NOT find TCMalloc")
    endif(TCMALLOC_FIND_REQUIRED)
endif(TCMALLOC_FOUND)

### LibHDFS3 ###

find_path(LIBHDFS3_INCLUDE_DIR NAMES hdfs/hdfs.h)
find_library(LIBHDFS3_LIBRARY NAMES hdfs3)
if(LIBHDFS3_INCLUDE_DIR AND LIBHDFS3_LIBRARY)
    set(LIBHDFS3_FOUND true)
endif(LIBHDFS3_INCLUDE_DIR AND LIBHDFS3_LIBRARY)
if(LIBHDFS3_FOUND)
    set(LIBHDFS3_DEFINITION "-DWITH_HDFS")
    if(NOT LIBHDFS3_FIND_QUIETLY)
        message (STATUS "Found libhdfs3:")
        message (STATUS "  (Headers)       ${LIBHDFS3_INCLUDE_DIR}")
        message (STATUS "  (Library)       ${LIBHDFS3_LIBRARY}")
        message (STATUS "  (Definition)    ${LIBHDFS3_DEFINITION}")
    endif(NOT LIBHDFS3_FIND_QUIETLY)
else(LIBHDFS3_FOUND)
    message(STATUS "Could NOT find libhdfs3")
endif(LIBHDFS3_FOUND)
if(WITHOUT_HDFS)
    unset(LIBHDFS3_FOUND)
    message(STATUS "Not using libhdfs3 due to WITHOUT_HDFS option")
endif(WITHOUT_HDFS)

### MongoDB ###

find_path(MONGOCLIENT_INCLUDE_DIR NAMES mongo)
find_library(MONGOCLIENT_LIBRARY NAMES mongoclient)
if (MONGOCLIENT_INCLUDE_DIR AND MONGOCLIENT_LIBRARY)
    set(MONGOCLIENT_FOUND true)
endif(MONGOCLIENT_INCLUDE_DIR AND MONGOCLIENT_LIBRARY)
if (MONGOCLIENT_FOUND)
    set(MONGOCLIENT_DEFINITION "-DWITH_MONGODB")
    message (STATUS "Found MongoClient:")
    message (STATUS "  (Headers)       ${MONGOCLIENT_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${MONGOCLIENT_LIBRARY}")
    message (STATUS "  (Definition)    ${MONGOCLIENT_DEFINITION}")
else(MONGOCLIENT_FOUND)
    message (STATUS "Could NOT find MongoClient")
endif(MONGOCLIENT_FOUND)
if(WITHOUT_MONGODB)
    unset(MONGOCLIENT_FOUND)
    message(STATUS "Not using MongoClient due to WITHOUT_MONGODB option")
endif(WITHOUT_MONGODB)

### RT ###

find_library(RT_LIBRARY NAMES rt)
if(RT_LIBRARY)
    set(RT_FOUND true)
else(RT_LIBRARY)
    set(RT_FOUND false)
endif(RT_LIBRARY)
if (RT_FOUND)
    message (STATUS "Found RT:")
    message (STATUS "  (Library)       ${RT_LIBRARY}")
else(RT_FOUND)
    message (STATUS "Could NOT find RT")
endif(RT_FOUND)

### Thrift ###

find_path(THRIFT_INCLUDE_DIR NAMES thrift/Thrift.h)
find_library(THRIFT_LIBRARY NAMES thrift)
if (THRIFT_INCLUDE_DIR AND THRIFT_LIBRARY)
    set(THRIFT_FOUND true)
endif(THRIFT_INCLUDE_DIR AND THRIFT_LIBRARY)
if (THRIFT_FOUND)
    set(THRIFT_DEFINITION "-DWITH_THRIFT")
    message (STATUS "Found Thrift:")
    message (STATUS "  (Headers)       ${THRIFT_INCLUDE_DIR}")
    message (STATUS "  (Library)       ${THRIFT_LIBRARY}")
    message (STATUS "  (Definition)    ${THRIFT_DEFINITION}")
else(THRIFT_FOUND)
    message (STATUS "Could NOT find Thrift")
endif(THRIFT_FOUND)
if(WITHOUT_THRIFT)
    unset(THRIFT_FOUND)
    message(STATUS "Not using Thrift due to WITHOUT_THRIFT option")
endif(WITHOUT_THRIFT)
