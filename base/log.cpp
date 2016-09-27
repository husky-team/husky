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

#include <fstream>
#include <iostream>
#include <mutex>
#include <string>

namespace husky {
namespace base {

std::mutex gPrintLock;

void log_msg(const std::string& msg, LOG_TYPE type) {
    std::string prefix;
    switch (type) {
    case LOG_TYPE::LOG_INFO: {
        prefix = "\033[1;32m[INFO] \033[0m";
        break;
    }
    case LOG_TYPE::LOG_DEBUG: {
        prefix = "\033[1;30m[DEBUG] \033[0m";
        break;
    }
    case LOG_TYPE::LOG_WARN: {
        prefix = "\033[1;34m[WARN] \033[0m";
        break;
    }
    case LOG_TYPE::LOG_ERROR: {
        prefix = "\033[1;31m[ERROR] \033[0m";
        break;
    }
    default: { break; }
    }

    gPrintLock.lock();
    std::cout << prefix << msg << "\n";
    gPrintLock.unlock();
}

void log_msg(const std::string& msg, LOG_TYPE type, const std::string& file_name) {
    std::string prefix;
    switch (type) {
    case LOG_TYPE::LOG_INFO: {
        prefix = "[INFO] ";
        break;
    }
    case LOG_TYPE::LOG_DEBUG: {
        prefix = "[DEBUG] ";
        break;
    }
    case LOG_TYPE::LOG_WARN: {
        prefix = "[WARN] ";
        break;
    }
    case LOG_TYPE::LOG_ERROR: {
        prefix = "[ERROR] ";
        break;
    }
    default: { break; }
    }

    gPrintLock.lock();
    std::ofstream log_file(file_name, std::ofstream::out | std::ofstream::app);
    log_file << prefix << msg << "\n";
    log_file.close();
    gPrintLock.unlock();
}

}  // namespace base
}  // namespace husky
