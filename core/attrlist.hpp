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
#include <utility>
#include <vector>

#include "base/exception.hpp"
#include "base/serialization.hpp"
#include "core/objlist_data.hpp"
#include "core/utils.hpp"

namespace husky {

using base::BinStream;

class AttrListBase {
   public:
    AttrListBase(const AttrListBase&) = delete;
    AttrListBase& operator=(const AttrListBase&) = delete;

    AttrListBase(AttrListBase&&) = delete;
    AttrListBase& operator=(AttrListBase&&) = delete;

    virtual size_t size() = 0;

   protected:
    AttrListBase() = default;
    virtual ~AttrListBase() = default;

    virtual void resize(const size_t size) = 0;
    virtual void reorder(std::vector<int>& order) = 0;
    virtual void move(const size_t dest, const size_t src) = 0;
    virtual void migrate(BinStream& bin, const size_t idx) = 0;
    virtual void process_bin(BinStream& bin, const size_t idx) = 0;

    template <typename ObjT>
    friend class ObjList;
};

template <typename ObjT, typename AttrT>
class AttrList : public AttrListBase {
   public:
    AttrList(const AttrList&) = delete;
    AttrList& operator=(const AttrList&) = delete;

    AttrList(AttrList&&) = delete;
    AttrList& operator=(AttrList&&) = delete;

    inline size_t size() override { return data_.size(); }

    std::vector<AttrT>& get_data() { return data_; }

    // Getter
    AttrT& get(const size_t idx) {
        size_t objlist_size = master_data_ptr_->get_size();
        if (idx >= objlist_size) {
            throw base::HuskyException("AttrList<T>::get error: index out of range");
        }
        if (idx >= data_.size()) {
            data_.resize(objlist_size);
        }
        return data_[idx];
    }

    AttrT& get(const ObjT& obj) {
        size_t idx = master_data_ptr_->index_of(&obj);
        return this->get(idx);
    }

    inline AttrT& operator[](const size_t idx) { return this->get(idx); }

    inline AttrT& operator[](const ObjT& obj) { return this->get(obj); }

    // Setter
    void set(const size_t idx, AttrT&& attr) {
        size_t objlist_size = master_data_ptr_->get_size();
        if (idx >= objlist_size) {
            throw base::HuskyException("AttrList<T>::set error: index out of range");
        }
        if (idx >= data_.size()) {
            data_.resize(objlist_size);
        }
        data_[idx] = std::move(attr);
    }

    void set(const size_t idx, const AttrT& attr) {
        size_t objlist_size = master_data_ptr_->get_size();
        if (idx >= objlist_size) {
            throw base::HuskyException("AttrList<T>::set error: index out of range");
        }
        if (idx >= data_.size()) {
            data_.resize(objlist_size);
        }
        data_[idx] = attr;
    }

    void set(const ObjT& obj, AttrT&& attr) {
        size_t idx = master_data_ptr_->index_of(&obj);
        this->set(idx, std::move(attr));
    }

    void set(const ObjT& obj, const AttrT& attr) {
        size_t idx = master_data_ptr_->index_of(&obj);
        this->set(idx, attr);
    }

   protected:
    AttrList() = default;
    virtual ~AttrList() = default;

    explicit AttrList(ObjListData<ObjT>* objlist_data_ptr) {
        master_data_ptr_ = objlist_data_ptr;
        data_.resize(master_data_ptr_->get_size());
    }

    inline void resize(const size_t size) override { data_.resize(size); }

    // Reorder the list according to a permutaion
    inline void reorder(std::vector<int>& order) override { reorder(data_, order); }

    // Move j_th data to i_th
    inline void move(const size_t dest, const size_t src) override { data_[dest] = std::move(data_[src]); }

    void reorder(std::vector<AttrT>& data, std::vector<int> order) {
        size_t src = 0, dest;
        do {
            dest = order[src];
            if (dest != -1) {
                if (dest == src) {
                    order[src] = -1;
                    ++src;
                    continue;
                } else if (order[dest] == src) {
                    std::swap(data[src], data[dest]);
                    order[src] = -1;
                    order[dest] = -1;
                    ++src;
                    continue;
                } else if (order[dest] != -1) {
                    std::swap(data[src], data[dest]);
                    order[src] = -1;
                    src = dest;
                    continue;
                }
            }
            // if dest == -1 or order[dest] == -1
            order[src] = -1;
            src = 0;
            while (order[src] == -1 && src < order.size()) {
                ++src;
            }
        } while (src < order.size());
    }

    void migrate(BinStream& bin, const size_t idx) override {
        if (idx >= data_.size()) {
            data_.resize(master_data_ptr_->get_vector_size());
        }
        bin << data_[idx];
    }

    void process_bin(BinStream& bin, const size_t idx) override {
        AttrT attr;
        bin >> attr;
        this->set(idx, std::move(attr));
    }

    std::vector<AttrT> data_;
    ObjListData<ObjT>* master_data_ptr_;

    template <typename T>
    friend class ObjList;
};
}  // namespace husky
