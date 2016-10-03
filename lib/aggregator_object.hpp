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

#include <functional>
#include <sstream>
#include <string>

#include "base/serialization.hpp"
#include "core/utils.hpp"

namespace husky {
namespace lib {

using base::BinStream;

// Create a new copy of b and assign the copy to a
// Especially useful when ValueType is smart pointer, like std::shared_ptr
template <typename ValueType>
void copy_assign(ValueType& a, const ValueType& b) {
    a = b;
}

// This operator is used in AggregatorObject::aggregate
// Try to use `+` is no `+=` is defined for ValueType
template <typename ValueType>
void operator+=(ValueType& a, const ValueType& b) {
    copy_assign(a, a + b);
}

// This is used in AggregatorObject::to_string
// This is used for printing some information of b
template <typename ValueType>
void to_stream(std::ostream& os, const ValueType& b) {
    os << &b;
}

class AggregatorState {
   public:
    AggregatorState();
    virtual ~AggregatorState();

    bool is_zero();
    void set_zero();
    void set_non_zero();

    bool is_updated();
    void set_updated();
    void set_non_updated();

    bool need_reset_each_iter();
    void set_keep_aggregate();
    void set_reset_each_iter();

    bool is_active();
    void set_active();
    void set_inactive();

    bool is_removed();
    virtual void set_removed();
    virtual void set_non_removed();

    void sync_option(const AggregatorState& b);

    virtual std::string to_string();

    virtual void load(BinStream& in) = 0;
    virtual void save(BinStream& out) = 0;
    virtual void aggregate(AggregatorState* b) = 0;
    virtual void prepare_value() = 0;

   private:
    char state_ = ~0;  // store states and options
};

template <typename ValueType>
class AggregatorObject final : public AggregatorState {
   private:
    typedef std::function<void(ValueType&, const ValueType&)> AggregateLambdaType;
    typedef std::function<void(ValueType&)> ZeroLambdaType;
    typedef std::function<void(BinStream&, ValueType&)> LoadLambdaType;
    typedef std::function<void(BinStream&, const ValueType&)> SaveLambdaType;

   public:
    AggregatorObject(const AggregateLambdaType& aggregate, const ZeroLambdaType& zero, const LoadLambdaType& load,
                     const SaveLambdaType& save)
        : aggregate_(aggregate),
          zero_(zero ? zero : [](ValueType& a) { copy_assign(a, ValueType()); }),
          load_(load),
          save_(save) {}

    AggregatorObject(const AggregatorObject<ValueType>& b) = delete;

    void zero(ValueType& a) { zero_(a); }

    virtual void load(BinStream& in) { load_(in, *value_); }

    virtual void save(BinStream& out) { save_(out, *value_); }

    ValueType& get_value() {
        prepare_value();
        return *value_;
    }

    void aggregate(const ValueType& b_value) {
        if (is_zero()) {
            set_non_zero();
            if (zero_ == nullptr) {
                copy_assign(*value_, b_value);
                return;
            }
            zero_(*value_);
        }
        aggregate(*value_, b_value);
    }

    void aggregate(const std::function<void(ValueType& value)>& lambda) {
        if (is_zero()) {
            set_non_zero();
            zero(*value_);
        }
        lambda(*value_);
    }

    inline AggregateLambdaType get_aggregate_lambda() { return aggregate_; }

    inline ZeroLambdaType get_zero_lambda() { return zero_; }

    inline LoadLambdaType get_load_lambda() { return load_; }

    inline SaveLambdaType get_save_lambda() { return save_; }

    // set_removed && set_inactive
    virtual void set_removed() {
        if (!is_removed()) {
            AggregatorState::set_removed();
            AggregatorState::set_inactive();
            delete value_;
            value_ = nullptr;
            aggregate_ = nullptr;
            zero_ = nullptr;
            load_ = nullptr;
            save_ = nullptr;
        }
    }

    ~AggregatorObject() {
        if (value_ != nullptr) {
            delete value_;
        }
    }

    // debug purpose
    virtual std::string to_string() {
        std::ostringstream oss;
        oss << AggregatorState::to_string() << ", value: ";
        lib::to_stream(oss, *value_);
        return oss.str();
    }

   protected:
    ValueType* value_ = new ValueType();

    void aggregate(ValueType& a, const ValueType& b) { aggregate_(a, b); }

   private:
    AggregateLambdaType aggregate_ = nullptr;
    ZeroLambdaType zero_ = nullptr;
    LoadLambdaType load_ = nullptr;
    SaveLambdaType save_ = nullptr;

    virtual void aggregate(AggregatorState* b) { aggregate(*(static_cast<AggregatorObject<ValueType>*>(b)->value_)); }

    virtual void prepare_value() {
        if (is_zero()) {
            set_non_zero();
            zero(*value_);
        }
    }
};

}  // namespace lib
}  // namespace husky
