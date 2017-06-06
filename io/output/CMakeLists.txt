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

file(GLOB io-output-src-file outputformat_base.cpp elasticsearch_outputformat.cpp)

if(MONGOCLIENT_FOUND)
    file(GLOB io-output-mongo-src-file mongodb_outputformat.cpp)
    list(APPEND io-output-src-file ${io-output-mongo-src-file})
endif(MONGOCLIENT_FOUND)

husky_cache_variable(io-output-src-file ${io-output-src-file})

add_library(output-objs OBJECT ${io-output-src-file})
husky_default_properties(output-objs)
