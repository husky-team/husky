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

#ifdef WITH_MONGODB

#include <map>
#include <string>
#include <vector>

#include "io/input/mongodb_split.hpp"

namespace husky {

using namespace io;

class MongoSplitAssigner {
   public:
    MongoSplitAssigner();
    void master_mongodb_req_handler();
    void master_mongodb_req_end_handler();
    void master_setup_handler();
    virtual ~MongoSplitAssigner();
    void set_auth(const std::string&, const std::string&);
    void reset_auth();

    void create_splits();
    MongoDBSplit answer(const std::string&, const std::string&);
    void recieve_end(MongoDBSplit& split);

   private:
    bool need_auth_ = false;
    int end_count_;
    int split_num_;
    std::string error_msg_;
    std::string ns_;
    std::string password_;
    std::string server_;
    std::string username_;

    std::map<std::string, std::string> shards_map_;
    std::vector<MongoDBSplit> splits_;
    std::vector<MongoDBSplit> splits_end_;
};

}  // namespace husky

#endif
