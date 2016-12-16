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

#include <string>
#include <vector>

#include "base/exception.hpp"
#include "core/hash_ring.hpp"
#include "core/utils.hpp"

namespace husky {

class WorkerInfo {
   public:
    inline int get_process_id() const {
        if (process_id_ == -1)
            throw base::HuskyException("process_id not set yet");
        return process_id_;
    }

    inline int get_process_id(int global_worker_id) const {
        auto p = global_to_proc_.find(global_worker_id);
        ASSERT_MSG(p != global_to_proc_.end(), "global_worker_id not found");
        return p->second;
    }

    inline int get_num_local_workers(int process_id) const {
        auto p = local_to_global_.find(process_id);
        ASSERT_MSG(p != local_to_global_.end(), "process_id not found");
        return p->second.size();
    }

    inline int get_num_local_workers() const { return get_num_local_workers(process_id_); }

    inline std::vector<int> get_tids_by_pid(int pid) const {
        auto p_local_to_global_map = local_to_global_.find(pid);
        std::vector<int> tids;
        tids.reserve(p_local_to_global_map->second.size());
        for (auto& lid_tid : p_local_to_global_map->second) {
            tids.push_back(lid_tid.second);
        }
        return tids;
    }

    inline std::vector<int> get_local_tids() const { return get_tids_by_pid(process_id_); }

    inline std::vector<int> get_pids() const { return {processes_.begin(), processes_.end()}; }

    inline const std::string& get_hostname(int process_id) const {
        if (hostname_[process_id].size() == 0) {
            throw base::HuskyException("Host name of the process not set or process does not exist");
        }
        return hostname_[process_id];
    }

    inline const std::vector<std::string> get_hostnames() const { return hostname_; }

    inline int get_num_processes() const { return processes_.size(); }

    inline int get_num_workers() const { return workers_.size(); }

    inline std::vector<int> get_global_tids() const { return {workers_.begin(), workers_.end()}; }

    inline HashRing* get_hash_ring() { return &hash_ring_; }

    inline int local_to_global_id(int process_id, int local_worker_id) const {
        auto p_local = local_to_global_.find(process_id);
        ASSERT_MSG(p_local != local_to_global_.end(), "process_id not found");
        auto p_global = p_local->second.find(local_worker_id);
        ASSERT_MSG(p_global != p_local->second.end(), "process_id, local_worker_id not found");
        return p_global->second;
    }

    inline int local_to_global_id(int local_worker_id) const {
        return local_to_global_id(process_id_, local_worker_id);
    }

    inline int global_to_local_id(int global_worker_id) const {
        auto p = global_to_local_.find(global_worker_id);
        ASSERT_MSG(p != global_to_local_.end(), "global_worker_id not found");
        return p->second;
    }

    // this method is to retrieve the largest global id in worker_info
    // It's useful when we create a sub-worker_info from worker_info
    inline int get_largest_tid() const {
        return largest_tid_;
    }

    void add_worker(int process_id, int global_worker_id, int local_worker_id, int num_hash_ranges = 1);

    void set_hostname(int process_id, const std::string& hostname = "");

    inline void set_process_id(int process_id) { process_id_ = process_id; }

   protected:
    std::unordered_map<int, int> global_to_proc_;
    std::vector<std::string> hostname_;
    std::unordered_map<int, std::unordered_map<int, int>> local_to_global_;
    std::unordered_map<int, int> global_to_local_;
    std::unordered_set<int> processes_;
    std::unordered_set<int> workers_;
    HashRing hash_ring_;
    int largest_tid_ = -1;
    int process_id_ = -1;
};

}  // namespace husky
