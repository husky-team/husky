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
    ASSERT_MSG(global_to_proc_.find(global_worker_id) == global_to_proc_.end(),
               "global_worker_id already exists in global_to_proc_");
    global_to_proc_.insert({global_worker_id, process_id});

    // set global_to_local_
    ASSERT_MSG(global_to_local_.find(global_worker_id) == global_to_local_.end(),
               "global_worker_id already exists in global_to_local_");
    global_to_local_.insert({global_worker_id, local_worker_id});

    // set local_to_global_
    ASSERT_MSG(local_to_global_[process_id].find(local_worker_id) == local_to_global_[process_id].end(),
               "{process_id, local_worker_id} already exists in local_to_global_");
    local_to_global_[process_id].insert({local_worker_id, global_worker_id});

    // set hash_ring_
    hash_ring_.insert(global_worker_id);

    // set processes and workers
    if (processes_.find(process_id) == processes_.end())
        processes_.insert(process_id);
    if (workers_.find(global_worker_id) == workers_.end())
        workers_.insert(global_worker_id);

    // set largest_tid
    if (global_worker_id > largest_tid_)
        largest_tid_ = global_worker_id;
}

void WorkerInfo::set_hostname(int process_id, const std::string& hostname) {
    if (hostname_.size() <= process_id)
        hostname_.resize(process_id + 1);
    hostname_[process_id] = hostname;
}

}  // namespace husky
