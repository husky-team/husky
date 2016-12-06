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

#include <set>
#include <vector>

#include "base/serialization.hpp"

namespace husky {

using base::BinStream;

class HashRing {
   public:
    /// Insert a worker thread into the hash ring
    void insert(int tid, int pid, int num_ranges = 1);

    /// Remove a worker thread from the hash ring
    void remove(int tid);

    /// Given a position on the hash ring, return the worker thread id
    int lookup(uint64_t pos) const;

    inline int get_num_workers() const { return global_tids_vector_.size(); }

    template <typename KeyT>
    int hash_lookup(const KeyT& key) const {
        // Maybe we just use std::hash<KeyT>() ??
        // uint64_t pos = ObjT::partition(key);
        uint64_t pos = std::hash<KeyT>()(key);
        return lookup(pos);
    }

    std::vector<int>& get_global_pids() { return global_pids_vector_; }
    const std::vector<int>& get_global_pids() const { return global_pids_vector_; }

    std::vector<int>& get_global_tids() { return global_tids_vector_; }
    const std::vector<int>& get_global_tids() const { return global_tids_vector_; }

    int get_num_processes() { return global_pids_vector_.size(); }

    int get_num_local_threads(int pid) const { return num_local_threads_[pid]; }

    friend BinStream& operator<<(BinStream& stream, HashRing& hash_ring);
    friend BinStream& operator>>(BinStream& stream, HashRing& hash_ring);

   protected:
    std::vector<int> global_tids_vector_;
    std::vector<int> global_pids_vector_;
    std::vector<int> num_local_threads_;
    std::vector<int> tid_to_pid_;
};

BinStream& operator<<(BinStream& stream, HashRing& hash_ring);
BinStream& operator>>(BinStream& stream, HashRing& hash_ring);

}  // namespace husky
