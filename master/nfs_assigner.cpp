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
#include <fstream>
#include <string>
#include <utility>

#include "master/nfs_assigner.hpp"

#include "core/constants.hpp"
#include "core/context.hpp"
#include "master/master.hpp"

namespace husky {
static NFSBlockAssigner nfs_block_assigner;

NFSBlockAssigner::NFSBlockAssigner() {
    Master::get_instance().register_main_handler(TYPE_LOCAL_BLK_REQ,
                                                 std::bind(&NFSBlockAssigner::master_main_handler, this));
    Master::get_instance().register_setup_handler(std::bind(&NFSBlockAssigner::master_setup_handler, this));
}

void NFSBlockAssigner::master_main_handler() {
    auto& master = Master::get_instance();
    auto resp_socket = master.get_socket();
    std::string url, host;
    BinStream stream = zmq_recv_binstream(resp_socket.get());
    stream >> url;

    std::pair<std::string, size_t> ret = answer(host, url);
    stream.clear();
    stream << ret.first << ret.second;

    zmq_sendmore_string(resp_socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(resp_socket.get());
    zmq_send_binstream(resp_socket.get(), stream);

    base::log_msg(host + " => " + ret.first + "@" + std::to_string(ret.second));
}

void NFSBlockAssigner::master_setup_handler() { num_workers_alive = Context::get_worker_info()->get_num_workers(); }

void NFSBlockAssigner::browse_local(const std::string& url) {
    // If url is a directory, recursively traverse all files in url
    // Then for all the files, calculate the blocks

    // TODO(all): here I assume that url is a file
    std::ifstream in(url, std::ios::ate | std::ios::binary);
    if (in) {
        file_size[url] = in.tellg();
        file_offset[url] = 0;
        finish_dict[url] = 0;
    }
}

std::pair<std::string, size_t> NFSBlockAssigner::answer(std::string& host, std::string& url) {
    if (finish_dict.find(url) == finish_dict.end())
        browse_local(url);

    std::pair<std::string, size_t> ret = {"", 0};  // selected_file, offset
    if (file_offset[url] < file_size[url]) {
        ret.first = url;
        ret.second = file_offset[url];
        file_offset[url] += local_block_size;
    } else {
        finish_dict[url] += 1;
    }
    return ret;
}

/// Return the number of workers who have finished reading the files in
/// the given url
int NFSBlockAssigner::get_num_finished(std::string& url) { return finish_dict[url]; }

/// Use this when all workers finish reading the files in url
void NFSBlockAssigner::finish_url(std::string& url) {
    file_size.erase(url);
    file_offset.erase(url);
    finish_dict.erase(url);
}

}  // namespace husky
