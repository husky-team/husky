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

#include "base/log.hpp"

#include <string>

#include "boost/filesystem.hpp"
#include "glog/logging.h"

#include "base/exception.hpp"

namespace husky {
namespace base {

void log_init(const char* program_name) {
    google::InitGoogleLogging(program_name);
    // FLAGS_stderrthreshold:
    // default is 2, which means only show error msg.
    // change it with 0, which makes all kinds of msg show up.
    FLAGS_stderrthreshold = 0;
}

bool log_to_dir(const std::string& dir) {
    boost::filesystem::path dir_path(dir);
    try {
        if (boost::filesystem::exists(dir_path)) {
            if (!boost::filesystem::is_directory(dir_path))
                throw HuskyException("Log dir: " + dir + " is not a directory!");
        } else {
            boost::filesystem::create_directories(dir_path);
        }
        // TODO(legend: check permission.
        // boost::filesystem::file_status result = boost::filesystem::status(dir_path);
        // printf("%o\n", result.permissions());
    } catch (boost::filesystem::filesystem_error& e) {
        throw HuskyException(std::string(e.what()));
    }
    FLAGS_log_dir = dir;
}

void log_info(const std::string& msg) { LOG(INFO) << msg; }

void log_warning(const std::string& msg) { LOG(WARNING) << msg; }

void log_error(const std::string& msg) { LOG(ERROR) << msg; }

void log_fatal(const std::string& msg) { LOG(FATAL) << msg; }

}  // namespace base
}  // namespace husky
