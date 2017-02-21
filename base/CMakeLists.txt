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

file(GLOB base-src-files
    counter_barrier.cpp
    generation_lock.cpp
    hash.cpp
    log.cpp
    assert.cpp
    disk_store.cpp
    serialization.cpp
    session_local.cpp
    thread_support.cpp)
husky_cache_variable(base-src-files ${base-src-files})

add_library(base-objs OBJECT ${base-src-files})
husky_default_properties(base-objs)

add_library(husky-base STATIC $<TARGET_OBJECTS:base-objs>)
