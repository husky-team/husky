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


# Short command for setting default properties
# Usage:
#   husky_default_properties(<target>)
function(husky_default_properties target)
    set_property(TARGET ${target} PROPERTY CXX_STANDARD 14)
    if (DEFINED external_project_dependencies)
        add_dependencies(${target} ${external_project_dependencies})
    endif()
endfunction()
