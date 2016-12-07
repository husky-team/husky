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

#include <map>
#include <set>
#include <string>
#include <vector>

#include "core/hash_ring.hpp"
#include "core/worker_info.hpp"

namespace husky {

class Config {
   public:
    Config();
    virtual ~Config();

    void set_master_host(const std::string& master_host);
    void set_master_port(const int& master_port);
    void set_comm_port(const int& comm_port);
    void set_param(const std::string& key, const std::string& value);

    inline std::string get_master_host() const { return master_host_; }
    inline int get_master_port() const { return master_port_; }
    inline int get_comm_port() const { return comm_port_; }
    inline int get_num_machines() const { return machines_.size(); }
    inline std::set<std::string> get_machines() { return machines_; }

    inline std::string get_param(const std::string& key) {
        std::map<std::string, std::string>::iterator ret = params_.find(key);
        if (ret == params_.end())
            return "";
        return ret->second;
    }

    bool init_with_args(int ac, char** av, const std::vector<std::string>& customized, HashRing* hash_ring = nullptr,
                        WorkerInfo* worker_info = nullptr);

   private:
    // TODO(legend): add boolean of valid.
    std::string master_host_ = "";
    int master_port_ = -1;
    int comm_port_ = -1;
    std::set<std::string> machines_;
    std::map<std::string, std::string> params_;
};

}  // namespace husky
