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

add_library(gradient-descent-objs OBJECT vector_linalg.cpp)
set_property(TARGET gradient-descent-objs PROPERTY CXX_STANDARD 14)

# Combine lib-objs
set(ml-objs $<TARGET_OBJECTS:gradient-descent-objs>)

# Visible to parent directory
set(ml-objs ${ml-objs} PARENT_SCOPE)
