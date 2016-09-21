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

#include "core/constants.hpp"
#include "core/context.hpp"
#include "io/hdfs_manager.hpp"
#include "master/hdfs_assigner.hpp"
#include "master/master.hpp"

namespace husky {

static HDFSBlockAssigner hdfs_block_assigner;

bool operator==(const BlkDesc& lhs, const BlkDesc& rhs) {
    return lhs.filename == rhs.filename && lhs.offset == rhs.offset && lhs.block_location == rhs.block_location;
}

HDFSBlockAssigner::HDFSBlockAssigner() {
    Master::get_instance().register_main_handler(TYPE_HDFS_BLK_REQ, std::bind(&HDFSBlockAssigner::master_main_handler, this));
    Master::get_instance().register_setup_handler(std::bind(&HDFSBlockAssigner::master_setup_handler, this));
}

void HDFSBlockAssigner::master_main_handler() {
    auto& master = Master::get_instance();
    auto* resp_socket = master.get_socket();
    std::string url, host;
    BinStream stream = zmq_recv_binstream(resp_socket);
    stream >> url >> host;

    std::pair<std::string, size_t> ret = answer(host, url);
    stream.clear();
    stream << ret.first << ret.second;

    zmq_sendmore_string(resp_socket, master.get_cur_client());
    zmq_sendmore_dummy(resp_socket);
    zmq_send_binstream(resp_socket, stream);

    base::log_msg(host+" => "+ret.first+"@"+std::to_string(ret.second));
}

void HDFSBlockAssigner::master_setup_handler() {
    init_hdfs(Context::get_param("hdfs_namenode"), Context::get_param("hdfs_namenode_port"));
    num_workers_alive = Context::get_worker_info()->get_num_workers();
}

void HDFSBlockAssigner::init_hdfs(const std::string& node, const std::string& port) {
    int num_retries = 3;
    while (num_retries--) {
        struct hdfsBuilder* builder = hdfsNewBuilder();
        hdfsBuilderSetNameNode(builder, node.c_str());
        hdfsBuilderSetNameNodePort(builder, std::stoi(port));
        fs_ = hdfsBuilderConnect(builder);
        hdfsFreeBuilder(builder);
        if (fs_)
            break;
    }
    if (fs_) {
        is_valid_ = true;
        return;
    }
    base::log_msg("Failed to connect to HDFS " + node + ":" + port);
}

void HDFSBlockAssigner::browse_hdfs(const std::string& url) {
    if (!fs_)
        return;

    int num_files;
    int dummy;
    hdfsFileInfo* file_info = hdfsListDirectory(fs_, url.c_str(), &num_files);
    auto& files_locality = files_locality_dict[url];
    for (int i = 0; i < num_files; ++i) {
        // for every file in a directory
        if (file_info[i].mKind != kObjectKindFile)
            continue;
        size_t k = 0;
        while (k < file_info[i].mSize) {
            // for every block in a file
            auto blk_loc = hdfsGetFileBlockLocations(fs_, file_info[i].mName, k, 1, &dummy);
            for (int j = 0; j < blk_loc->numOfNodes; ++j) {
                // for every replication in a block
                files_locality.insert(
                    BlkDesc{std::string(file_info[i].mName) + '\0', k, std::string(blk_loc->hosts[j])});
            }
            k += file_info[i].mBlockSize;
        }
    }
    hdfsFreeFileInfo(file_info, num_files);
}

std::pair<std::string, size_t> HDFSBlockAssigner::answer(const std::string& host, const std::string& url) {
    if (!fs_)
        return {"", 0};

    // cannot find url
    if (files_locality_dict.find(url) == files_locality_dict.end()) {
        browse_hdfs(url);
        finish_dict[url] = 0;
    }

    // empty url
    auto& files_locality = files_locality_dict[url];
    if (files_locality.size() == 0) {
        finish_dict[url] += 1;
        if (finish_dict[url] == num_workers_alive)
            files_locality_dict.erase(url);
        return {"", 0};
    }

    std::pair<std::string, size_t> ret = {"", 0};  // selected_file, offset
    for (auto& triplet : files_locality)
        if (triplet.block_location == host) {
            ret = {triplet.filename, triplet.offset};
            break;
        }

    if (ret.first == "")
        ret = {files_locality.begin()->filename, files_locality.begin()->offset};

    // remove
    for (auto it = files_locality.begin(); it != files_locality.end();) {
        if (it->filename == ret.first && it->offset == ret.second)
            it = files_locality.erase(it);
        else
            it++;
    }

    return ret;
}

/// Return the number of workers who have finished reading the files in
/// the given url
int HDFSBlockAssigner::get_num_finished(std::string& url) { return finish_dict[url]; }

/// Use this when all workers finish reading the files in url
void HDFSBlockAssigner::finish_url(std::string& url) { files_locality_dict.erase(url); }

}  // namespace husky
#endif
