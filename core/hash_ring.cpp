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

void HashRing::insert(int tid, int num_ranges) {
    for (int i = 0; i < num_ranges; i++)
        global_tids_vector_.push_back(tid);
}

void HashRing::remove(int tid) {
    global_tids_vector_.erase(std::remove(global_tids_vector_.begin(), global_tids_vector_.end(), tid),
                              global_tids_vector_.end());
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

BinStream& operator<<(BinStream& stream, HashRing& hash_ring) { return stream << hash_ring.global_tids_vector_; }

BinStream& operator>>(BinStream& stream, HashRing& hash_ring) { return stream >> hash_ring.global_tids_vector_; }

}  // namespace husky
