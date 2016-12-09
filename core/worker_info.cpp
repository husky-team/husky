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

#include <algorithm>
#include <string>

namespace husky {

void WorkerInfo::add_worker(int process_id, int global_worker_id, int local_worker_id, int num_hash_ranges) {
    // set global_to_proc_
    if (global_to_proc_.size() <= global_worker_id)
        global_to_proc_.resize(global_worker_id + 1, -1);
    global_to_proc_[global_worker_id] = process_id;

    // set global_to_local_
    if (global_to_local_.size() <= global_worker_id)
        global_to_local_.resize(global_worker_id + 1);
    global_to_local_[global_worker_id] = local_worker_id;

    // set local_to_global_
    if (local_to_global_.size() <= process_id)
        local_to_global_.resize(process_id + 1);

    if (local_to_global_[process_id].size() <= local_worker_id)
        local_to_global_[process_id].resize(local_worker_id + 1);
    local_to_global_[process_id][local_worker_id] = global_worker_id;

    // set hash_ring_
    hash_ring_.insert(global_worker_id);

    // set processes and workers
    if (std::find(processes.begin(), processes.end(), process_id) == processes.end())
        processes.push_back(process_id);
    if (std::find(workers.begin(), workers.end(), global_worker_id) == workers.end())
        workers.push_back(global_worker_id);
}

void WorkerInfo::set_hostname(int process_id, const std::string& hostname) {
    if (hostname_.size() <= process_id)
        hostname_.resize(process_id + 1);
    hostname_[process_id] = hostname;
}

}  // namespace husky
