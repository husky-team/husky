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

#include <mutex>
#include <unordered_map>
#include <vector>

#include "core/accessor.hpp"

namespace husky {

class AccessorSetBase {
   public:
    virtual ~AccessorSetBase() {}
};

template <typename CollectT>
class AccessorSet : public AccessorSetBase {
   public:
    std::vector<Accessor<CollectT>> data;
};

class AccessorFactory {
   public:
    template <typename CollectT>
    static std::vector<Accessor<CollectT>>* create_accessor(size_t channel_id, size_t local_id,
                                                            size_t num_local_threads) {
        // double-checked locking
        if (accessors_map.find(channel_id) == accessors_map.end()) {
            std::lock_guard<std::mutex> lock(accessors_map_mutex);
            if (accessors_map.find(channel_id) == accessors_map.end()) {
                AccessorSet<CollectT>* accessor_set = new AccessorSet<CollectT>();
                accessor_set->data.resize(num_local_threads);
                for (auto& i : accessor_set->data) {
                    i.init(num_local_threads);
                }
                AccessorFactory::num_local_threads.insert(std::make_pair(channel_id, num_local_threads));
                accessors_map.insert(std::make_pair(channel_id, accessor_set));
            }
        }
        auto& data = dynamic_cast<AccessorSet<CollectT>*>(accessors_map[channel_id])->data;
        // data[local_id].init();
        return &data;
    }

    static void remove_accessor(size_t channel_id) {
        std::lock_guard<std::mutex> lock(accessors_map_mutex);
        num_local_threads[channel_id] -= 1;
        if (num_local_threads[channel_id] == 0) {
            delete accessors_map[channel_id];
            accessors_map.erase(channel_id);
            num_local_threads.erase(channel_id);
        }
    }

   protected:
    static std::unordered_map<size_t, AccessorSetBase*> accessors_map;
    static std::mutex accessors_map_mutex;
    static std::unordered_map<size_t, size_t> num_local_threads;
};

}  // namespace husky
