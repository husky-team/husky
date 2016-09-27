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

#ifdef WITH_HDFS
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "boost/ptr_container/ptr_map.hpp"

#include "hdfs/hdfs.h"

namespace husky {
namespace io {

class HDFSManager {
   public:
    HDFSManager() = default;
    HDFSManager(const std::string&, const std::string&, bool rm_original = true);
    ~HDFSManager();

    void create_file(const std::string&, const int&, const int&);
    void write(const std::string&, const std::string&, const int&);
    void flush(const std::string&, const int&);
    void flush_all_files();
    void close_all_files();

    inline hdfsFS& get_fs() { return fs_; }

   protected:
    bool rm_original_;
    hdfsFS fs_ = NULL;
    std::unordered_map<std::string, hdfsFile> opened_files[1000];
};

namespace HDFS {

typedef boost::ptr_map<std::string, HDFSManager> ManagerMap;
extern thread_local ManagerMap sManagers;

void Write(const std::string& host, const std::string& port, const std::string& content, const std::string& dest_url,
           const int& worker_id);
void Write(const std::string& host, const std::string& port, const char* content, const size_t& len,
           const std::string& dest_url, const int& worker_id);
void CloseFile(const std::string& host, const std::string& port);

}  // namespace HDFS

}  // namespace io
}  // namespace husky
#endif
