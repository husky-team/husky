// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>

namespace husky {
namespace base {

void log_init(const char* program_name);
bool log_to_dir(const std::string& dir);

/// Make sure to call `log_init` before these functions.
void log_info(const std::string& msg);
void log_warning(const std::string& msg);
void log_error(const std::string& msg);
void log_fatal(const std::string& msg);

//TODO(legend): may port more functions here from glog.

}  // namespace base
}  // namespace husky
