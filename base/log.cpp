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
#ifdef _MSC_VER
#define GOOGLE_GLOG_DLL_DECL
#define GLOG_NO_ABBREVIATED_SEVERITIES
#endif
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
    // FLAGS_colorlogtostderr: support color in term.
    FLAGS_colorlogtostderr = true;
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
        // TODO(legend): check permission.
        // boost::filesystem::file_status result = boost::filesystem::status(dir_path);
        // printf("%o\n", result.permissions());
    } catch (boost::filesystem::filesystem_error& e) {
        throw HuskyException(std::string(e.what()));
    }
    FLAGS_log_dir = dir;
}

HuskyLogger::HuskyLogger(const char* file, int line, int severity)
    : file_(file), line_(line), severity_(severity), log_stream_() {}

HuskyLogger::~HuskyLogger() { google::LogMessage(file_.c_str(), line_, severity_).stream() << log_stream_.str(); }

std::stringstream& HuskyLogger::stream() { return log_stream_; }

void log_msg(const std::string& msg) { husky::LOG_I << "Note: log_msg() is deprecated.\n " << msg; }

}  // namespace base
}  // namespace husky
