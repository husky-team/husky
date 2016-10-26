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

#include "core/hash_ring.hpp"

#include <algorithm>
#include <random>

#include "base/serialization.hpp"

namespace husky {

void HashRing::insert(int tid, int pid, int num_ranges) {
    global_tids_vector_.push_back(tid);
    if (std::count(global_pids_vector_.begin(), global_pids_vector_.end(), pid) == 0)
        global_pids_vector_.push_back(pid);
    if (num_local_threads_.size() < pid + 1)
        num_local_threads_.resize(pid + 1);
    num_local_threads_[pid] += 1;
    if (tid_to_pid_.size() < tid + 1)
        tid_to_pid_.resize(tid + 1);
    tid_to_pid_[tid] = pid;
}

void HashRing::remove(int tid) {
    // TODO(Fan): remove from global_pids_vector_ as well
    if (std::find(global_tids_vector_.begin(), global_tids_vector_.end(), tid) == global_tids_vector_.end())
        return;
    global_tids_vector_.erase(std::remove(global_tids_vector_.begin(), global_tids_vector_.end(), tid),
                              global_tids_vector_.end());
    int pid = tid_to_pid_[tid];
    tid_to_pid_[tid] = -1;
    num_local_threads_[pid] -= 1;
    if (num_local_threads_[pid] == 0)
        global_pids_vector_.erase(std::remove(global_pids_vector_.begin(), global_pids_vector_.end(), pid),
                                  global_pids_vector_.end());
}

int HashRing::lookup(uint64_t pos) const {
    int64_t b = 1, j = 0;
    while (j < get_num_workers()) {
        b = j;
        pos = pos * 2862933555777941757ULL + 1;
        j = (b + 1) * (static_cast<double>(1LL << 31) / static_cast<double>((pos >> 33) + 1));
    }
    return global_tids_vector_[b];
}

BinStream& operator<<(BinStream& stream, HashRing& hash_ring) {
    stream << hash_ring.global_tids_vector_;
    stream << hash_ring.global_pids_vector_;
    stream << hash_ring.num_local_threads_;
    stream << hash_ring.tid_to_pid_;
    return stream;
}

BinStream& operator>>(BinStream& stream, HashRing& hash_ring) {
    stream >> hash_ring.global_tids_vector_;
    stream >> hash_ring.global_pids_vector_;
    stream >> hash_ring.num_local_threads_;
    stream >> hash_ring.tid_to_pid_;
    return stream;
}

}  // namespace husky
