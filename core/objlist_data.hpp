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
#include <random>
#include <vector>

#include "boost/random.hpp"

#include "base/exception.hpp"
#include "base/serialization.hpp"

namespace husky {

using base::BinStream;

template <typename ObjT>
class ObjListData {
   public:
    ObjListData() = default;
    ~ObjListData() = default;

    ObjListData(const ObjListData&) = delete;
    ObjListData& operator=(const ObjListData&) = delete;

    ObjListData(ObjListData&&) = delete;
    ObjListData& operator=(ObjListData&&) = delete;

    inline size_t get_size() const { return data_.size() - num_del_; }
    inline size_t get_vector_size() const { return data_.size(); }

    void clear() { data_.clear(); }

    void shuffle() {
        std::random_device rd;
        boost::random::mt19937 gen(rd());
        std::shuffle(data_.begin(), data_.end(), gen);
    }

    // Find the index of an obj
    size_t index_of(const ObjT* const obj_ptr) const {
        size_t idx = obj_ptr - &data_[0];
        if (idx < 0 || idx >= data_.size())
            throw base::HuskyException("ObjListData<T>::index_of error: index out of range");
        return idx;
    }

    friend BinStream& operator<<(BinStream& stream, ObjListData<ObjT>& obj_list_data) {
        return stream << obj_list_data.data_;
    }

    friend BinStream& operator>>(BinStream& stream, ObjListData<ObjT>& obj_list_data) {
        return stream >> obj_list_data.data_;
    }

   private:
    std::vector<ObjT> data_;
    size_t num_del_ = 0;

    template <typename T>
    friend class ObjList;
};

}  // namespace husky
