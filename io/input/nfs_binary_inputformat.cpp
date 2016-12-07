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

#include "io/input/nfs_binary_inputformat.hpp"

#include <fstream>
#include <string>

#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/network.hpp"
#include "core/utils.hpp"
#include "io/input/binary_inputformat_impl.hpp"

namespace husky {
namespace io {

NFSFileBinStream::NFSFileBinStream(const std::string& filename)
    : FileBinStreamBase(filename), file_(filename, std::ifstream::in | std::ifstream::binary) {
    ASSERT_MSG(file_.good() && file_.is_open(), ("NFSFileBinStream fails to read from " + filename).c_str());
    file_.seekg(0, file_.end);
    file_size_ = file_.tellg();
    file_.seekg(0, file_.beg);
}

size_t NFSFileBinStream::size() const { return file_size_; }

void* NFSFileBinStream::pop_front_bytes(size_t sz) {
    void* res = FileBinStreamBase::pop_front_bytes(sz);
    file_size_ -= sz;
    return res;
}

NFSFileBinStream::~NFSFileBinStream() { file_.close(); }

void NFSFileBinStream::read(char* buf, size_t sz) { file_.read(buf, sz); }

void NFSFileAsker::init(const std::string& path, const std::string& filter) {
    if (!filter.empty() || path.find(':') == std::string::npos) {
        file_url_ = path + ":" + filter;
    } else {
        file_url_ = path;
    }
}

std::string NFSFileAsker::fetch_new_file() {
    base::BinStream request;
    request << get_hostname() << file_url_;
    base::BinStream response = Context::get_coordinator()->ask_master(request, TYPE_NFS_FILE_REQ);
    return base::deser<std::string>(response);
}

void NFSBinaryInputFormat::set_input(const std::string& path, const std::string& filter) {
    asker_.init(path, filter);
    this->to_be_setup();
}

// Should not used by user
bool NFSBinaryInputFormat::next(BinaryInputFormatImpl::RecordT& record) {
    std::string file_name = asker_.fetch_new_file();
    if (file_name.empty()) {
        return false;
    }
    record.set_bin_stream(new NFSFileBinStream(file_name));
    return true;
}

}  // namespace io
}  // namespace husky
