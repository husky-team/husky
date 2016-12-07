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

#include "io/input/hdfs_binary_inputformat.hpp"

#include <string>

#include "hdfs/hdfs.h"

#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/network.hpp"
#include "core/utils.hpp"
#include "io/input/binary_inputformat_impl.hpp"

namespace husky {
namespace io {

HDFSFileBinStream::HDFSFileBinStream(hdfsFS fs, const std::string& filename) : FileBinStreamBase(filename) {
    fs_ = fs;
    file_ = hdfsOpenFile(fs_, filename.c_str(), O_RDONLY, 0, 0, 0);
    ASSERT_MSG(file_ != nullptr, ("HDFSFileBinStream fails to read from " + filename).c_str());
    hdfsFileInfo* info = hdfsGetPathInfo(fs_, filename.c_str());
    file_size_ = info->mSize;
    hdfsFreeFileInfo(info, 1);
}

size_t HDFSFileBinStream::size() const { return file_size_; }

void* HDFSFileBinStream::pop_front_bytes(size_t sz) {
    void* res = FileBinStreamBase::pop_front_bytes(sz);
    file_size_ -= sz;
    return res;
}

HDFSFileBinStream::~HDFSFileBinStream() {
    ASSERT_MSG(hdfsCloseFile(fs_, file_) == 0, ("Failed to close a hdfs file: " + get_filename()).c_str());
    file_ = nullptr;
}

void HDFSFileBinStream::read(char* buf, size_t sz) {
    for (size_t i = 0; i < sz; i += hdfsRead(fs_, file_, buf + i, sz - i)) {
    }
}

void HDFSFileAsker::init(const std::string& path, const std::string& filter) {
    if (!filter.empty() || path.find(':') == std::string::npos) {
        file_url_ = path + ":" + filter;
    } else {
        file_url_ = path;
    }
}

std::string HDFSFileAsker::fetch_new_file() {
    base::BinStream request;
    request << get_hostname() << file_url_;
    base::BinStream response = Context::get_coordinator()->ask_master(request, TYPE_HDFS_FILE_REQ);
    return base::deser<std::string>(response);
}

HDFSBinaryInputFormat::HDFSBinaryInputFormat() {
    struct hdfsBuilder* builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, Context::get_param("hdfs_namenode").c_str());
    int port;
    try {
        port = std::stoi(Context::get_param("hdfs_namenode_port").c_str());
    } catch (...) {
        ASSERT_MSG(false, ("Failed to parse hdfs namenode port: " + Context::get_param("hdfs_namenode_port")).c_str());
    }
    hdfsBuilderSetNameNodePort(builder, port);
    fs_ = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
}

HDFSBinaryInputFormat::~HDFSBinaryInputFormat() { hdfsDisconnect(fs_); }

void HDFSBinaryInputFormat::set_input(const std::string& path, const std::string& filter) {
    asker_.init(path, filter);
    this->to_be_setup();
}

// Should not used by user
bool HDFSBinaryInputFormat::next(BinaryInputFormatImpl::RecordT& record) {
    std::string file_name = asker_.fetch_new_file();
    if (file_name.empty()) {
        return false;
    }
    record.set_bin_stream(new HDFSFileBinStream(fs_, file_name));
    return true;
}

}  // namespace io
}  // namespace husky
