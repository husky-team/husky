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

#ifdef WITH_ORC

#include "master/orc_assigner.hpp"

#include <fstream>
#include <string>
#include <utility>

#include "boost/filesystem.hpp"
#include "orc/OrcFile.hh"

#include "base/log.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "io/input/orc_hdfs_inputstream.hpp"
#include "master/master.hpp"

namespace husky {

using orc::Reader;
using orc::ReaderOptions;
using orc::createReader;
using orc::readLocalFile;
using orc::ColumnVectorBatch;

static ORCBlockAssigner orc_block_assigner;

ORCBlockAssigner::ORCBlockAssigner() {
    Master::get_instance().register_main_handler(TYPE_ORC_BLK_REQ,
                                                 std::bind(&ORCBlockAssigner::master_main_handler, this));
    Master::get_instance().register_setup_handler(std::bind(&ORCBlockAssigner::master_setup_handler, this));
}

void ORCBlockAssigner::master_main_handler() {
    auto& master = Master::get_instance();
    auto resp_socket = master.get_socket();
    std::string url;
    BinStream stream = zmq_recv_binstream(resp_socket.get());
    stream >> url;

    std::pair<std::string, size_t> ret = answer(url);
    stream.clear();
    stream << ret.first << ret.second;

    zmq_sendmore_string(resp_socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(resp_socket.get());
    zmq_send_binstream(resp_socket.get(), stream);

    LOG_I << " => " << ret.first << "@" << ret.second;
}

void ORCBlockAssigner::master_setup_handler() {
    init_hdfs(Context::get_param("hdfs_namenode"), Context::get_param("hdfs_namenode_port"));
    num_workers_alive = Context::get_num_workers();
}

void ORCBlockAssigner::init_hdfs(const std::string& node, const std::string& port) {
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
        return;
    }
    LOG_I << "Failed to connect to HDFS " << node << ":" << port;
}

void ORCBlockAssigner::browse_hdfs(const std::string& url) {
    if (!fs_)
        return;

    try {
        int num_files;
        size_t total = 0;
        auto& files = files_dict[url];
        hdfsFileInfo* file_info = hdfsListDirectory(fs_, url.c_str(), &num_files);
        for (int i = 0; i < num_files; ++i) {
            // for every file in a directory
            if (file_info[i].mKind != kObjectKindFile)
                continue;
            ReaderOptions opts;
            reader = createReader(io::read_hdfs_file(fs_, file_info[i].mName), opts);
            size_t num_rows = reader->getNumberOfRows();
            files.push_back(OrcFileDesc{std::string(file_info[i].mName) + '\0', num_rows, 0});
            total += num_rows;
        }
        LOG_I << "Total num of rows: " << total;
        hdfsFreeFileInfo(file_info, num_files);
    } catch (const std::exception& ex) {
        LOG_I << "Exception cought: " << ex.what();
    }
}

/**
 * @return selected_file, offset
 */
std::pair<std::string, size_t> ORCBlockAssigner::answer(std::string& url) {
    if (!fs_) {
        return {"", 0};
    }
    // Directory or file status initialization
    // This condition is true either when the begining of the file or
    // all the workers has finished reading this file or directory
    if (files_dict.find(url) == files_dict.end()) {
        browse_hdfs(url);
        finish_dict[url] = 0;
    }

    // empty url
    auto& files = files_dict[url];
    if (files.empty()) {
        finish_dict[url] += 1;
        if (finish_dict[url] == num_workers_alive) {
            files_dict.erase(url);
        }
        return {"", 0};
    }

    auto& file = files.back();
    std::pair<std::string, size_t> ret = {file.filename, file.offset};
    file.offset += io::kOrcRowBatchSize;

    // remove
    if (file.offset > file.num_of_rows)
        files.pop_back();

    return ret;
}

}  // namespace husky

#endif
