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

#include <sstream>
#include <string>

namespace husky {

#define LOG_I base::HuskyLogger(__FILE__, __LINE__, 0).stream()
#define LOG_W base::HuskyLogger(__FILE__, __LINE__, 1).stream()
#define LOG_E base::HuskyLogger(__FILE__, __LINE__, 2).stream()
#define LOG_F base::HuskyLogger(__FILE__, __LINE__, 3).stream()

#ifdef HUSKY_DEBUG_MODE

#define DLOG_I LOG_I
#define DLOG_W LOG_W
#define DLOG_E LOG_E
#define DLOG_F LOG_F

#else  // HUSKY_DEBUG_MODE

#define DLOG_I base::HuskyLoggerVoidify().void_stream
#define DLOG_W base::HuskyLoggerVoidify().void_stream
#define DLOG_E base::HuskyLoggerVoidify().void_stream
#define DLOG_F base::HuskyLoggerVoidify().void_stream

#endif  // HUSKY_DEBUG_MODE

namespace base {

/// Not include glog/logging.h to avoid namespace pollution.
/// Please use LOG_* instead of constructing HuskyLogger.

void log_init(const char* program_name);
bool log_to_dir(const std::string& dir);

/// Make sure to call `log_init` before log functions.

class HuskyLogger {
   public:
    HuskyLogger() = delete;
    HuskyLogger(const char* file, int line, int severity);
    virtual ~HuskyLogger();

    std::stringstream& stream();

   private:
    std::string file_;
    int line_;
    int severity_;
    std::stringstream log_stream_;
};

class HuskyLoggerVoidify {
   public:
    std::stringstream void_stream;
    void operator&(std::ostream&) {}
};

// Deprecated. It would not show the real location of the calling.
void log_msg(const std::string& msg);

// TODO(legend): may port more functions here from glog.

}  // namespace base
}  // namespace husky
