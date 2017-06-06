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

#include <deque>
#include <map>
#include <string>

namespace husky {

class ESShardAssigner {
   public:
    ESShardAssigner();
    void master_elasticsearch_req_handler();
    //  void master_elasticsearch_req_end_handler();
    //  void master_setup_handler();
    virtual ~ESShardAssigner();

    void create_shards();
    std::string answer(const std::string&, const std::string&, std::string node);
    // void recieve_end(MongoDBSplit& split);

   private:
    int shard_num_;

    std::string server_;
    std::string index_;
    std::deque<std::string> nodes_;
    std::deque<std::string> shards_;
};

}  // namespace husky
