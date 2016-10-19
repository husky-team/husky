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

#include "master/nfs_binary_assigner.hpp"

#include <regex>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/filesystem.hpp"

#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "master/master.hpp"

namespace husky {
static NFSFileAssigner nfs_file_assigner;

NFSFileAssigner::NFSFileAssigner() {
    Master::get_instance().register_main_handler(TYPE_NFS_FILE_REQ, std::bind(&NFSFileAssigner::response, this));
    Master::get_instance().register_setup_handler(std::bind(&NFSFileAssigner::setup, this));
}

void NFSFileAssigner::setup() {}

void NFSFileAssigner::response() {
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

std::string NFSFileAssigner::answer(const std::string& fileurl) {
    if (file_infos_.count(fileurl) == 0) {
        prepare(fileurl);
    }
    NFSFileInfo& info = file_infos_[fileurl];
    if (info.files_to_assign.empty()) {
        if (++info.finish_count == Context::get_worker_info()->get_num_workers()) {
            file_infos_.erase(fileurl);
        }
        return "";  // no file to assign
    }
    // choose a file
    std::string file = info.files_to_assign.back();
    info.files_to_assign.pop_back();
    return info.base + file;
}

void NFSFileAssigner::prepare(const std::string& url) {
    size_t first_colon = url.find(":");
    first_colon = first_colon == std::string::npos ? url.length() : first_colon;
    std::string base = url.substr(0, first_colon);
    std::string filter = first_colon >= url.length() - 1 ? ".*" : url.substr(first_colon + 1, url.length());
    auto base_path = boost::filesystem::path(base);
    ASSERT_MSG(boost::filesystem::exists(base_path), ("Given path not found on NFS: " + base).c_str());
    if (boost::filesystem::is_directory(base_path)) {
        NFSFileInfo& info = file_infos_[url];
        info.base = base_path.string();
        if (info.base.back() != boost::filesystem::path::preferred_separator)
            info.base.push_back(boost::filesystem::path::preferred_separator);
        std::vector<std::string>& files = info.files_to_assign;
        size_t base_len = info.base.length();
        try {
            std::regex filter_regex(filter);
            for (auto& p : boost::filesystem::recursive_directory_iterator(base_path)) {
                if (boost::filesystem::is_regular_file(p)) {
                    std::string filename = p.path().string().substr(base_len);
                    if (std::regex_match(filename, filter_regex)) {
                        files.push_back(filename);
                    }
                }
            }
        } catch (std::regex_error e) {
            ASSERT_MSG(false, (std::string("Illegal regex expression(") + e.what() + "): " + filter).c_str());
        }
    } else if (boost::filesystem::is_regular_file(base_path)) {
        file_infos_[url].files_to_assign.push_back(base);
    } else {
        ASSERT_MSG(false, ("Given base path is neither a file nor a directory: " + base).c_str());
    }
}

}  // namespace husky
