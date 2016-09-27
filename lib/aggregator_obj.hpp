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

#include <string>

#include "base/serialization.hpp"

namespace husky {
namespace lib {

using base::BinStream;

class AggregatorState {
   public:
    AggregatorState();
    virtual ~AggregatorState();

    bool is_non_zero();
    bool need_update();
    virtual std::string to_string();

    void set_zero();
    void set_non_zero();
    void set_no_update();
    void set_need_update();
    void to_keep_aggregate();
    void to_reset_each_round();

    virtual void load(BinStream& in) = 0;
    virtual void save(BinStream& out) = 0;
    virtual void aggregate(AggregatorState* b) = 0;

   private:
    char flag_;
};

/* *
 * Assumptions of the value:
 * 1. it supports operator =
 * 2. it has empty constructor
 * */
template <typename ValueType>
class AggregatorObject : public AggregatorState {
   public:
    typedef ValueType ValueT;

    AggregatorObject() {}

    explicit AggregatorObject(const ValueT& value) : value_(value) {}

    virtual void aggregate(ValueT& a, const ValueT& b) = 0;

    virtual void zero(ValueT& v) { v = ValueT(); }

    ValueT& get_value() {
        if (!is_non_zero()) {
            zero(value_);
            set_non_zero();
        }
        return value_;
    }

   protected:
    void aggregate(AggregatorState* b) {
        if (is_non_zero()) {
            aggregate(value_, static_cast<AggregatorObject<ValueT>*>(b)->value_);
        } else {
            new_copy(value_, static_cast<AggregatorObject<ValueT>*>(b)->value_);
            set_non_zero();
        }
    }

    void load(BinStream& in) { in >> value_; }

    void save(BinStream& out) { out << value_; }

   private:
    ValueT value_;
};

}  // namespace lib
}  // namespace husky
