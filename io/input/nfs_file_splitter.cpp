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

#include "io/input/nfs_file_splitter.hpp"

#include <string>

#include "base/thread_support.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/coordinator.hpp"

namespace husky {
namespace io {

int NFSFileSplitter::nfs_block_size = 8 * 1024 * 1024;

NFSFileSplitter::NFSFileSplitter() {
    auto blk_size = husky::Context::get_param("nfs_block_size");
    if (!blk_size.empty())
        NFSFileSplitter::nfs_block_size = std::stoi(blk_size);
    data = new char[nfs_block_size];
}

NFSFileSplitter::~NFSFileSplitter() { delete[] data; }

void NFSFileSplitter::load(std::string url) { this->url = url; }

boost::string_ref NFSFileSplitter::fetch_block(bool is_next) {
    int nbytes;
    if (is_next) {
        if (fin.eof())
            return "";
        nbytes = fin.readsome(data, nfs_block_size);
    } else {
        base::BinStream question;
        question << url;
        base::BinStream answer = husky::Context::get_coordinator().ask_master(question, husky::TYPE_LOCAL_BLK_REQ);
        std::string fn;
        answer >> fn;
        answer >> offset;
        if (fn == "") {
            return "";
        }
        nbytes = read_block(fn);
    }
    return boost::string_ref(data, nbytes);
}

size_t NFSFileSplitter::get_offset() { return offset; }

int NFSFileSplitter::read_block(std::string const& fn) {
    if (cur_fn != fn) {
        if (fin.is_open())
            fin.close();
        fin.open(fn);
    }
    fin.seekg(offset);
    return fin.readsome(data, nfs_block_size);
}

}  // namespace io
}  // namespace husky
