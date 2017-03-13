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

#ifdef WITH_ORC

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "orc/OrcFile.hh"

#include "io/input/orc_hdfs_inputstream.hpp"
#include "master/master.hpp"

namespace husky {

struct OrcFileDesc {
    std::string filename;
    size_t num_of_rows;
    size_t offset;
};

class ORCBlockAssigner {
   public:
    ORCBlockAssigner();
    ~ORCBlockAssigner() = default;

    void master_main_handler();
    void master_setup_handler();
    void init_hdfs(const std::string& node, const std::string& port);
    void browse_hdfs(const std::string& url);
    std::pair<std::string, size_t> answer(std::string& url);

   private:
    hdfsFS fs_ = NULL;
    // int row_batch_size = 8 * 1024;
    int num_workers_alive;
    // std::map<std::string, size_t> file_offset;
    // std::map<std::string, size_t> file_size;
    std::map<std::string, std::vector<OrcFileDesc>> files_dict;
    std::map<std::string, int> finish_dict;
    std::unique_ptr<orc::Reader> reader;
};
}  // namespace husky

#endif
