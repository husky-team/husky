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

namespace husky {

class WorkerInfo {
   public:
    inline int get_process_id() const {
        if (process_id_ == -1)
            throw base::HuskyException("process_id not set yet");
        return process_id_;
    }

    inline int get_process_id(int global_worker_id) const { return global_to_proc_[global_worker_id]; }

    inline int get_num_local_workers() const { return local_to_global_[process_id_].size(); }

    inline int get_num_local_workers(int process_id) const { return local_to_global_[process_id].size(); }

    inline const std::vector<int>& get_tids_by_pid(int pid) const { return local_to_global_[pid]; }

    inline const std::vector<int>& get_local_tids() const { return local_to_global_[process_id_]; }

    inline const std::vector<int>& get_pids() const { return processes; }

    inline const std::string& get_hostname(int process_id) const {
        if (hostname_[process_id].size() == 0) {
            throw base::HuskyException("Host name of the process not set or process does not exist");
        }
        return hostname_[process_id];
    }

    inline int get_num_processes() const { return processes.size(); }

    inline int get_num_workers() const { return workers.size(); }

    inline const std::vector<int>& get_global_tids() const { return workers; }

    HashRing* get_hash_ring() { return &hash_ring_; }

    inline int local_to_global_id(int process_id, int local_worker_id) const {
        return local_to_global_[process_id][local_worker_id];
    }

    void add_worker(int process_id, int global_worker_id, int local_worker_id, int num_hash_ranges = 1);

    void set_hostname(int process_id, const std::string& hostname = "");

    inline void set_process_id(int process_id) { process_id_ = process_id; }

   protected:
    std::vector<int> global_to_proc_;
    std::vector<std::string> hostname_;
    std::vector<std::vector<int>> local_to_global_;
    std::vector<int> processes;
    std::vector<int> workers;
    HashRing hash_ring_;
    int process_id_ = -1;
};

}  // namespace husky
