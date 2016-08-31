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

#include <random>

#include "base/serialization.hpp"

namespace husky {

void HashRing::insert(int worker_id, int num_ranges) { worker_set_.insert(worker_id); }

void HashRing::remove(int worker_id) { worker_set_.erase(worker_id); }

int HashRing::lookup(uint64_t pos) const {
    int64_t b = 1, j = 0;
    while (j < get_num_workers()) {
        b = j;
        pos = pos * 2862933555777941757ULL + 1;
        j = (b + 1) * (static_cast<double>(1LL << 31) / static_cast<double>((pos >> 33) + 1));
    }
    return b;
}

BinStream& operator<<(BinStream& stream, HashRing& hash_ring) {
    stream << hash_ring.worker_set_.size();
    for (auto& wid : hash_ring.worker_set_)
        stream << wid;
    return stream;
}

BinStream& operator>>(BinStream& stream, HashRing& hash_ring) {
    size_t sz;
    stream >> sz;
    hash_ring.worker_set_.clear();
    for (int i = 0; i < sz; i++) {
        int wid;
        stream >> wid;
        hash_ring.worker_set_.insert(wid);
    }
    return stream;
}

}  // namespace husky
