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

#include "master/hdfs_binary_assigner.hpp"

#include <regex>
#include <string>
#include <unordered_map>
#include <vector>

#include "hdfs/hdfs.h"

#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "master/master.hpp"

namespace husky {
static HDFSFileAssigner hdfs_file_assigner;

HDFSFileAssigner::HDFSFileAssigner() {
    Master::get_instance().register_main_handler(TYPE_HDFS_FILE_REQ, std::bind(&HDFSFileAssigner::response, this));
    Master::get_instance().register_setup_handler(std::bind(&HDFSFileAssigner::setup, this));
}

void HDFSFileAssigner::setup() {}

void HDFSFileAssigner::response() {
    auto& master = Master::get_instance();
    auto socket = master.get_socket();
    base::BinStream request = zmq_recv_binstream(socket.get());
    std::string host = base::deser<std::string>(request);
    std::string fileurl = base::deser<std::string>(request);
    std::string filename = answer(fileurl);
    base::BinStream response;
    response << filename;
    zmq_sendmore_string(socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(socket.get());
    zmq_send_binstream(socket.get(), response);

    if (filename.empty())
        filename = "None";
    base::log_msg(host + " => " + fileurl + "@" + filename);
}

std::string HDFSFileAssigner::answer(const std::string& fileurl) {
    if (file_infos_.count(fileurl) == 0) {
        prepare(fileurl);
    }
    HDFSFileInfo& info = file_infos_[fileurl];
    std::vector<std::string>& files = info.files_to_assign;
    if (files.empty()) {
        if (++info.finish_count == Context::get_num_workers()) {
            file_infos_.erase(fileurl);
        }
        return "";  // no file to assign
    }
    // choose a file
    std::string file = files.back();
    files.pop_back();
    return info.base + file;
}

void HDFSFileAssigner::prepare(const std::string& url) {
    size_t first_colon = url.find(":");
    first_colon = first_colon == std::string::npos ? url.length() : first_colon;
    std::string base = url.substr(0, first_colon);
    std::string filter = first_colon >= url.length() - 1 ? ".*" : url.substr(first_colon + 1, url.length());

    struct hdfsBuilder* builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, Context::get_param("hdfs_namenode").c_str());
    int port;
    try {
        port = std::stoi(Context::get_param("hdfs_namenode_port").c_str());
    } catch (...) {
        ASSERT_MSG(false, ("Failed to parse hdfs namenode port: " + Context::get_param("hdfs_namenode_port")).c_str());
    }
    hdfsBuilderSetNameNodePort(builder, port);
    hdfsFS fs = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
    hdfsFileInfo* base_info = hdfsGetPathInfo(fs, base.c_str());

    ASSERT_MSG(base_info != nullptr, ("Given path not found on HDFS: " + base).c_str());

    if (base_info->mKind == kObjectKindDirectory) {
        HDFSFileInfo& info = file_infos_[url];
        std::vector<std::string>& files = info.files_to_assign;
        info.base = std::string(base_info->mName);
        if (info.base.back() != '/')
            info.base.push_back('/');
        int base_len = base.length();
        try {
            std::regex filter_regex(filter);
            std::function<void(const char*)> recursive_hdfs_visit = nullptr;
            recursive_hdfs_visit = [&fs, &filter, &files, &filter_regex, base_len,
                                    &recursive_hdfs_visit](const char* base) {
                int num_entries;
                hdfsFileInfo* infos = hdfsListDirectory(fs, base, &num_entries);
                for (int i = 0; i < num_entries; ++i) {
                    if (infos[i].mKind == kObjectKindFile) {
                        std::string filename = std::string(infos[i].mName).substr(base_len);
                        if (std::regex_match(filename, filter_regex)) {
                            files.push_back(filename);
                        }
                    } else if (infos[i].mKind == kObjectKindDirectory) {
                        recursive_hdfs_visit(infos[i].mName);
                    }
                }
                hdfsFreeFileInfo(infos, num_entries);
            };
            recursive_hdfs_visit(base.c_str());
        } catch (std::regex_error e) {
            ASSERT_MSG(false, (std::string("Illegal regex expression(") + e.what() + "): " + filter).c_str());
        }
        hdfsFreeFileInfo(base_info, 1);
        hdfsDisconnect(fs);
    } else if (base_info->mKind == kObjectKindFile) {
        hdfsFreeFileInfo(base_info, 1);
        file_infos_[url].files_to_assign.push_back(base);
        hdfsDisconnect(fs);
    } else {
        hdfsFreeFileInfo(base_info, 1);
        hdfsDisconnect(fs);
        ASSERT_MSG(false, ("Given base path is neither a file nor a directory: " + base).c_str());
    }
}

}  // namespace husky
