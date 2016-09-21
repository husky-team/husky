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

#ifdef WITH_HDFS

#include <map>
#include <string>
#include <unordered_set>
#include <utility>

#include "hdfs/hdfs.h"

namespace husky {

struct BlkDesc {
    std::string filename;
    size_t offset;
    std::string block_location;
};

bool operator==(const BlkDesc& lhs, const BlkDesc& rhs);

class HDFSBlockAssigner {
   public:
    HDFSBlockAssigner();
    ~HDFSBlockAssigner() = default;

    void master_main_handler();
    void master_setup_handler();
    inline bool is_valid() const { return is_valid_; }
    void init_hdfs(const std::string& node, const std::string& port);
    void browse_hdfs(const std::string& url);
    std::pair<std::string, size_t> answer(const std::string& host, const std::string& url);
    /// Return the number of workers who have finished reading the files in
    /// the given url
    int get_num_finished(std::string& url);
    /// Use this when all workers finish reading the files in url
    void finish_url(std::string& url);

    int num_workers_alive;

   private:
    bool is_valid_ = false;
    hdfsFS fs_ = NULL;
    std::map<std::string, int> finish_dict;
    std::map<std::string, std::unordered_set<BlkDesc>> files_locality_dict;
};

}  // namespace husky

namespace std {
template <>
struct hash<husky::BlkDesc> {
    size_t operator()(const husky::BlkDesc& t) const { return hash<string>()(t.filename); }
};
}  // namespace std

#endif
