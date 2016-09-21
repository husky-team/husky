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

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <vector>

#include "base/exception.hpp"
#include "core/channel/channel_destination.hpp"
#include "core/channel/channel_source.hpp"
#include "core/utils.hpp"

namespace husky {

class BaseObjList : public ChannelSource, public ChannelDestination {
   public:
    virtual ~BaseObjList() {}
};

template <typename ObjT>
class ObjList : public BaseObjList {
   public:
    // TODO(all): should be protected. The list should be constructed by possibly Context
    ObjList() {}

    virtual ~ObjList() {}

    std::vector<ObjT>& get_data() { return data; }

    // Sort the objlist
    void sort() {
        if (data.size() == 0)
            return;
        std::sort(data.begin(), data.end(), [](const ObjT& a, const ObjT& b) { return a.id() < b.id(); });
        hashed_objs.clear();
        sorted_size = data.size();
    }

    // TODO(Fan): This will invalidate the object dict
    void deletion_finalize() {
        if (data.size() == 0)
            return;
        size_t i = 0, j;
        // move i to the first empty place
        while (i < data.size() && !del_bitmap[i])
            i++;

        if (i == data.size())
            return;

        for (j = data.size() - 1; j > 0; j--) {
            if (!del_bitmap[j]) {
                data[i] = std::move(data[j]);
                i += 1;
                // move i to the next empty place
                while (i < data.size() && !del_bitmap[i])
                    i++;
            }
            if (i >= j)
                break;
        }
        data.resize(j);
        del_bitmap.resize(j);
        num_del = 0;
        std::fill(del_bitmap.begin(), del_bitmap.end(), 0);
    }

    // Delete an object
    void delete_object(const ObjT* const obj_ptr) {
        // TODO(all): Decide whether we can remove this
        // if (unlikely(del_bitmap.size() < data.size())) {
        //     del_bitmap.resize(data.size());
        // }
        // lazy operation
        auto idx = obj_ptr - &data[0];
        if (idx < 0 || idx >= data.size())
            throw HuskyException("ObjList<T>::delete_object error: index out of range");
        del_bitmap[idx] = true;
        num_del += 1;
    }

    // Find obj according to key
    // @Return a pointer to obj
    ObjT* find(const typename ObjT::KeyT& key) {
        auto& working_list = this->data;
        ObjT* start_addr = &working_list[0];
        int r = this->sorted_size - 1;
        int l = 0;
        int m = (r + l) / 2;
        if (working_list.size() == 0)
            return nullptr;
        while (l <= r) {
// __builtin_prefetch(start_addr+(m+1+r)/2, 0, 1);
// __builtin_prefetch(start_addr+(l+m-1)/2, 0, 1);
#ifdef ENABLE_LIST_FIND_PREFETCH
            __builtin_prefetch(&(start_addr[(m + 1 + r) / 2].id()), 0, 1);
            __builtin_prefetch(&(start_addr[(l + m - 1) / 2].id()), 0, 1);
#endif
            // __builtin_prefetch(&working_list[(m+1+r)/2], 0, 1);
            // __builtin_prefetch(&working_list[(l+m-1)/2], 0, 1);
            auto tmp = start_addr[m].id();
            if (tmp == key) {
                return &working_list[m];
            } else if (tmp < key) {
                l = m + 1;
            } else {
                r = m - 1;
            }
            m = (r + l) / 2;
        }

        // The object to find is not in the sorted part
        if ((sorted_size < data.size()) && (hashed_objs.find(key) != hashed_objs.end())) {
            return &(this->data[hashed_objs[key]]);
        }
        return nullptr;
    }

    // Find the index of an obj
    // @Return idx if found
    // @Return -1 otherwise
    size_t index_of(const ObjT* const obj_ptr) const {
        size_t idx = obj_ptr - &data[0];
        if (idx < 0 || idx >= data.size())
            throw HuskyException("ObjList<T>::index_of error: index out of range");
        return idx;
    }

    // Add an object
    void add_object(ObjT&& obj) {
        hashed_objs[obj.id()] = data.size();
        data.push_back(std::move(obj));
        del_bitmap.push_back(0);
    }
    void add_object(const ObjT& obj) {
        hashed_objs[obj.id()] = data.size();
        data.push_back(obj);
        del_bitmap.push_back(0);
    }
    size_t add_object_get_index(ObjT&& obj) {
        size_t ret = hashed_objs[obj.id()] = data.size();
        data.push_back(std::move(obj));
        del_bitmap.push_back(0);
        return ret;
    }
    size_t add_object_get_index(const ObjT& obj) {
        size_t ret = hashed_objs[obj.id()] = data.size();
        data.push_back(obj);
        del_bitmap.push_back(0);
        return ret;
    }

    // Check a certain position of del_bitmap
    // @Return True if it's deleted
    bool get_del(size_t idx) const { return del_bitmap[idx]; }

    // getter
    inline size_t get_sorted_size() const { return sorted_size; }
    inline size_t get_num_del() const { return num_del; }
    inline size_t get_hashed_size() const { return hashed_objs.size(); }
    inline size_t get_size() const { return data.size() - num_del; }
    inline size_t get_vector_size() const { return data.size(); }
    inline ObjT& get(size_t i) { return data[i]; }

   protected:
    std::vector<ObjT> data;
    std::vector<bool> del_bitmap;
    size_t sorted_size = 0;
    size_t num_del = 0;
    std::unordered_map<typename ObjT::KeyT, size_t> hashed_objs;
};

}  // namespace husky
