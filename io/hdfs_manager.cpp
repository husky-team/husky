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

#ifdef WITH_HDFS

#include "io/hdfs_manager.hpp"

#include <string>

#include "base/exception.hpp"
#include "core/utils.hpp"

namespace husky {
namespace io {

namespace HDFS {

thread_local ManagerMap sManagers;

void Write(const std::string& host, const std::string& port, const std::string& content, const std::string& dest_url,
           const int& worker_id) {
    std::string hdfs = host + port;
    if (sManagers.find(hdfs) == sManagers.end())
        sManagers.insert(hdfs, new io::HDFSManager(host, port));
    sManagers[hdfs].write(content, dest_url, worker_id);
    sManagers[hdfs].flush(dest_url, worker_id);
}

void Write(const std::string& host, const std::string& port, const char* content, const size_t& len,
           const std::string& dest_url, const int& worker_id) {
    std::string hdfs = host + port;
    if (sManagers.find(hdfs) == sManagers.end())
        sManagers.insert(hdfs, new io::HDFSManager(host, port));
    sManagers[hdfs].write(std::string(content, len), dest_url, worker_id);
    sManagers[hdfs].flush(dest_url, worker_id);
}

void CloseFile(const std::string& host, const std::string& port) {
    std::string hdfs = host + port;
    if (sManagers.find(hdfs) != sManagers.end())
        sManagers[hdfs].close_all_files();
}

}  // namespace HDFS

HDFSManager::HDFSManager(const std::string& host, const std::string& port, bool rm_original) {
    rm_original_ = rm_original;
    int num_retries = 3;
    while (num_retries--) {
        struct hdfsBuilder* builder = hdfsNewBuilder();
        hdfsBuilderSetNameNode(builder, host.c_str());
        hdfsBuilderSetNameNodePort(builder, std::stoi(port));
        fs_ = hdfsBuilderConnect(builder);
        hdfsFreeBuilder(builder);
        if (!fs_)
            continue;
        return;
    }
    ASSERT_MSG(fs_ != NULL, ("Failed to connect to HDFS " + host + ":" + port).c_str());
}

HDFSManager::~HDFSManager() {
    close_all_files();
    hdfsDisconnect(fs_);
}

void HDFSManager::create_file(const std::string& url, const int& flags, const int& worker_id) {
    // if (this->rm_original_ && 0 == hdfsExists(fs, url.c_str())) {
    //     hdfsDelete(fs, url.c_str(), true);//delete url recursively
    // }
    int created = hdfsCreateDirectory(fs_, url.c_str());
    ASSERT_MSG(created != -1, ("Failed to create HDFS directory " + url).c_str());
    std::string fn = url + "/part-" + std::to_string(worker_id);
    auto file = hdfsOpenFile(fs_, fn.c_str(), flags, 0, 0, 0);
    for (int i = 0; i < 10; i++) {
        if (file == NULL)
            file = hdfsOpenFile(fs_, fn.c_str(), flags, 0, 0, 0);
    }
    assert(file != NULL);
    opened_files[worker_id][url] = file;
}

void HDFSManager::write(const std::string& content, const std::string& url, const int& worker_id) {
    if (opened_files[worker_id].find(url) == opened_files[worker_id].end())
        create_file(url, O_WRONLY, worker_id);

    auto file = opened_files[worker_id][url];
    assert(hdfsFileIsOpenForWrite(file));
    int numWritten = hdfsWrite(fs_, file, content.c_str(), content.size());
}

void HDFSManager::flush(const std::string& url, const int& worker_id) {
    if (!fs_ || opened_files[worker_id].find(url) == opened_files[worker_id].end())
        return;

    auto file = opened_files[worker_id][url];
    int num_retries = 3;
    while (num_retries--) {
        int rc = hdfsFlush(fs_, file);
        if (rc == 0)
            return;
    }
    throw base::HuskyException("Failed to flush to HDFS");
}

void HDFSManager::flush_all_files() {
    if (!fs_)
        return;

    for (auto& dict : opened_files)
        for (auto& pair : dict)
            hdfsFlush(fs_, pair.second);
}

void HDFSManager::close_all_files() {
    for (auto& dict : opened_files) {
        for (auto& pair : dict) {
            hdfsFlush(fs_, pair.second);
            hdfsCloseFile(fs_, pair.second);
        }
        dict.clear();
    }
}

}  // namespace io
}  // namespace husky
#endif
