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

namespace husky {

class WorkerInfo {
   public:
    inline int get_num_workers() const { return num_workers_; }
    inline int get_num_processes() const { return num_proc_; }
    inline int get_proc_id() const { return proc_id_; }
    inline int get_proc_id(int global_worker_id) const { return global_to_proc_[global_worker_id]; }

    inline int get_num_local_workers(int proc_id) const { return local_to_global_[proc_id].size(); }

    inline int get_num_local_workers() const { return local_to_global_[proc_id_].size(); }

    inline const std::vector<int>& get_tids_by_pid(int pid) const { return local_to_global_[pid]; }

    inline const std::string& get_host(int proc_id) const { return host_[proc_id]; }

    inline int local_to_global_id(int proc_id, int local_worker_id) const {
        return local_to_global_[proc_id][local_worker_id];
    }

    void set_num_processes(int num_proc);
    void set_num_workers(int num_workers);
    void set_proc_id(int proc_id);

    void add_worker(int proc_id, int global_worker_id, int local_worker_id);
    void add_proc(int proc_id, const std::string& hostname);

   protected:
    std::vector<int> global_to_proc_;
    std::vector<std::string> host_;
    std::vector<std::vector<int>> local_to_global_;
    int num_proc_ = -1;
    int num_workers_ = -1;
    int proc_id_ = -1;
};

}  // namespace husky
