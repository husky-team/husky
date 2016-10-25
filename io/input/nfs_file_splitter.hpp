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

#pragma once

#include <fstream>
#include <string>

#include "boost/utility/string_ref.hpp"

#include "io/input/file_splitter_base.hpp"

namespace husky {
namespace io {

class NFSFileSplitter final : public FileSplitterBase {
   public:
    NFSFileSplitter();
    virtual ~NFSFileSplitter();

    void load(std::string url);
    virtual boost::string_ref fetch_block(bool is_next = false);
    inline size_t get_offset() { return offset_; }

   protected:
    int read_block(std::string const& fn);

    std::ifstream fin_;
    std::string url_;
    std::string cur_fn;
    size_t offset_;
    char* data_;

    static int nfs_block_size;
};

}  // namespace io
}  // namespace husky
