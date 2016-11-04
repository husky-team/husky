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

#include <cassert>
#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "boost/tokenizer.hpp"
#include "core/engine.hpp"

namespace husky {
namespace lib {
namespace ml {
using vec_double = std::vector<double>;

template <typename FT, typename LT>
class FeatureLabelBase {
   public:
    // Object key
    using KeyT = int;
    KeyT key;
    const KeyT& id() const { return key; }

    // constructors
    FeatureLabelBase() = default;
    explicit FeatureLabelBase(FT& _feature, LT& _label, const KeyT& k) : feature_(_feature), label_(_label), key(k) {}

    // get feature and label by reference
    inline FT& use_feature() { return this->feature_; }
    inline LT& use_label() { return this->label_; }

    // get feature and label by value
    inline FT get_feature() const { return this->feature_; }
    inline LT get_label() const { return this->label_; }

    // serialization
    friend BinStream& operator<<(BinStream& stream, const FeatureLabelBase& this_obj) {
        return stream << this_obj.key << this_obj.feature_ << this_obj.label_;
    }
    friend BinStream& operator>>(BinStream& stream, FeatureLabelBase& this_obj) {
        return stream >> this_obj.key >> this_obj.feature_ >> this_obj.label_;
    }

   protected:
    FT feature_;
    LT label_;
};  // FeatureLabelBase

typedef FeatureLabelBase<std::vector<double>, double> FeatureLabel;
typedef FeatureLabelBase<std::vector<std::pair<int, double>>, double> SparseFeatureLabel;
}  // namespace ml
}  // namespace lib
}  // namespace husky
