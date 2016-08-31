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

#include "core/worker_info.hpp"

#include <string>

namespace husky {

void WorkerInfo::set_num_processes(int num_proc) { num_proc_ = num_proc; }

void WorkerInfo::set_num_workers(int num_workers) { num_workers_ = num_workers; }

void WorkerInfo::set_proc_id(int proc_id) { proc_id_ = proc_id; }

void WorkerInfo::add_worker(int proc_id, int global_worker_id, int local_worker_id) {
    if (global_to_proc_.size() <= global_worker_id)
        global_to_proc_.resize(global_worker_id + 1);
    global_to_proc_[global_worker_id] = proc_id;

    if (local_to_global_.size() <= proc_id)
        local_to_global_.resize(proc_id + 1);

    if (local_to_global_[proc_id].size() <= local_worker_id)
        local_to_global_[proc_id].resize(local_worker_id + 1);
    local_to_global_[proc_id][local_worker_id] = global_worker_id;
}

void WorkerInfo::add_proc(int proc_id, const std::string& hostname) {
    if (host_.size() <= proc_id)
        host_.resize(proc_id + 1);
    host_[proc_id] = hostname;
}

}  // namespace husky
