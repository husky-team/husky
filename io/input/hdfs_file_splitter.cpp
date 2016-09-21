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

#include "io/input/hdfs_file_splitter.hpp"

#include <string>

#include "base/thread_support.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/coordinator.hpp"

namespace io {

using base::BinStream;

size_t HDFSFileSplitter::hdfs_block_size = 0;

HDFSFileSplitter::HDFSFileSplitter() : data_(nullptr) {}

HDFSFileSplitter::~HDFSFileSplitter() {
    if (data_)
        delete[] data_;
}

void HDFSFileSplitter::init_blocksize(hdfsFS fs_, const std::string& url) {
    int num_files;
    auto file_info = hdfsListDirectory(fs_, url.c_str(), &num_files);
    for (int i = 0; i < num_files; ++i) {
        if (file_info[i].mKind == kObjectKindFile) {
            hdfs_block_size = file_info[i].mBlockSize;
            hdfsFreeFileInfo(file_info, num_files);
            return;
        }
        continue;
    }
    throw std::runtime_error("Block size init error. (File NOT exist or EMPTY directory)");
}

void HDFSFileSplitter::load(std::string url) {
    // init url, fs_, hdfs_block_size
    url_ = url;
    // init fs_
    struct hdfsBuilder* builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, husky::Context::get_param("hdfs_namenode").c_str());
    hdfsBuilderSetNameNodePort(builder, std::stoi(husky::Context::get_param("hdfs_namenode_port")));
    fs_ = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
    base::call_once_each_time(init_blocksize, fs_, url_);
    data_ = new char[hdfs_block_size];
}

boost::string_ref HDFSFileSplitter::fetch_block(bool is_next) {
    int nbytes = 0;
    if (is_next) {
        // directly read the next block using the current file
        nbytes = hdfsRead(fs_, file_, data_, hdfs_block_size);
        if (nbytes == 0)
            return "";
        if (nbytes == -1) {
            throw std::runtime_error("read next block error");
        }
    } else {
        // Ask the master for a new block
        BinStream question;
        question << url_ << husky::Context::get_param("hostname");
        BinStream answer = husky::Context::get_coordinator().ask_master(question, husky::TYPE_HDFS_BLK_REQ);
        std::string fn;
        answer >> fn;
        answer >> offset_;

        if (fn == "") {
            // no more files
            return "";
        }

        if (file_ != NULL) {
            int rc = hdfsCloseFile(fs_, file_);
            assert(rc == 0);
            // Notice that "file" will be deleted inside hdfsCloseFile
            file_ = NULL;
        }

        // read block
        nbytes = read_block(fn);
    }
    return boost::string_ref(data_, nbytes);
}

size_t HDFSFileSplitter::get_offset() { return offset_; }

int HDFSFileSplitter::read_block(const std::string& fn) {
    file_ = hdfsOpenFile(fs_, fn.c_str(), O_RDONLY, 0, 0, 0);
    assert(file_ != NULL);
    hdfsSeek(fs_, file_, offset_);
    size_t start = 0;
    size_t nbytes = 0;
    while (start < hdfs_block_size) {
        // only 128KB per hdfsRead
        nbytes = hdfsRead(fs_, file_, data_ + start, hdfs_block_size);
        start += nbytes;
        if (nbytes == 0)
            break;
    }
    return start;
}

}  // namespace io
