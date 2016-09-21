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

#include "hdfs/hdfs.h"

namespace io {

class HDFSFileSplitter {
   public:
    HDFSFileSplitter();
    virtual ~HDFSFileSplitter();

    static void init_blocksize(hdfsFS fs, const std::string& url);
    virtual void load(std::string url);
    virtual boost::string_ref fetch_block(bool is_next = false);
    size_t get_offset();

   protected:
    int read_block(const std::string& fn);

    // using heap
    char* data_;
    // url may be a directory, so that cur_file is different from url
    hdfsFile file_ = NULL;
    hdfsFS fs_;
    size_t offset_ = 0;
    std::string url_;

    static size_t hdfs_block_size;
};

}  // namespace io
