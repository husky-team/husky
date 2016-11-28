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

#include "core/executor.hpp"
#include "core/objlist.hpp"
#include "core/utils.hpp"
#include "lib/aggregator.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/vector.hpp"

namespace husky {
namespace lib {
namespace ml {

// get max absolute value
template <typename T, bool is_sparse>
void set_max(Vector<T, false>& va, const Vector<T, is_sparse>& vb) {
    ASSERT_MSG(va.get_feature_num() >= vb.get_feature_num(), "Range does not match");
    for (auto it = vb.begin_feaval(); it != vb.end_feaval(); ++it) {
        const auto& entry = *it;
        va[entry.fea] = std::max(va[entry.fea], std::abs(entry.val));
    }
}

// overload operator/= and operator*= for scaling
template <typename T>
void operator/=(Vector<T, false>& va, const Vector<T, false>& vb) {
    for (auto it = va.begin_feaval(); it != va.end_feaval(); ++it) {
        auto entry = *it;
        if (vb[entry.fea] != 0) {
            entry.val /= vb[entry.fea];
        }
    }
}
template <typename T>
void operator/=(Vector<T, true>& va, const Vector<T, false>& vb) {
    for (auto it = va.begin_feaval(); it != va.end_feaval(); ++it) {
        auto& entry = *it;
        if (vb[entry.fea] != 0) {
            entry.val /= vb[entry.fea];
        }
    }
}

template <typename T>
void operator*=(Vector<T, true>& va, const Vector<T, false>& vb) {
    for (auto it = va.begin_feaval(); it != va.end_feaval(); ++it) {
        auto& entry = *it;
        if (vb[entry.fea] != 0) {
            entry.val *= vb[entry.fea];
        }
    }
}
template <typename T>
void operator*=(Vector<T, false>& va, const Vector<T, false>& vb) {
    for (auto it = va.begin_feaval(); it != va.end_feaval(); ++it) {
        auto entry = *it;
        if (vb[entry.fea] != 0) {
            entry.val *= vb[entry.fea];
        }
    }
}

// scaling each element in vector to [-1, 1]
template <typename FeatureT, typename LabelT, bool is_sparse>
class LinearScaler {
    typedef LabeledPointHObj<FeatureT, LabelT, is_sparse> ObjT;
    typedef ObjList<ObjT> ObjL;
    typedef Vector<FeatureT, false> VecT;

   public:
    explicit LinearScaler(int _num_features) : num_features_(_num_features) {
        if (_num_features > 0)
            max_X_ = VecT(_num_features, 0.0);
    }

    void fit(ObjL& data) {
        ASSERT_MSG(num_features_ > 0, "Non-positive number of parameters.");
        lib::Aggregator<VecT> max_X_agg(max_X_, [](VecT& va, const VecT& vb) { set_max(va, vb); },
                                        [this](VecT& va) { va = VecT(this->num_features_, 0.0); });
        lib::Aggregator<LabelT> max_y_agg(max_y_, [](LabelT& a, const LabelT& b) { a = std::max(a, std::abs(b)); });
        auto& ch = lib::AggregatorFactory::get_channel();

        list_execute(data, {}, {&ch}, [&](ObjT& this_obj) {
            max_X_agg.update_any([&, this](VecT& va) { set_max(va, this_obj.x); });
            max_y_agg.update(this_obj.y);
        });

        max_X_ = max_X_agg.get_value();
        max_y_ = max_y_agg.get_value();
    }

    template <typename... Args>
    void fit(ObjL& data, Args&... args) {
        fit(data);
        fit(args...);
    }

    void transform(ObjL& data) {
        list_execute(data, [&](ObjT& this_obj) {
            this_obj.x /= max_X_;
            if (max_y_ > 0)
                this_obj.y /= max_y_;
        });
    }

    template <typename... Args>
    void transform(ObjL& data, Args&... args) {
        transform(data);
        transform(args...);
    }

    void fit_transform(ObjL& data) {
        fit(data);
        transform(data);
    }

    template <typename... Args>
    void fit_transform(ObjL& data, Args&... args) {
        fit(data, args...);
        transform(data, args...);
    }

    void inverse_transform(ObjL& data) {
        list_execute(data, [&](ObjT& this_obj) {
            this_obj.x *= max_X_;
            if (max_y_ > 0)
                this_obj.y *= max_y_;
        });
    }

    template <typename... Args>
    void inverse_transform(ObjL& data, Args&... args) {
        inverse_transform(data);
        inverse_transform(args...);
    }

    Vector<FeatureT, false> get_max_X() { return this->max_X_; }
    LabelT get_max_y() { return this->max_y_; }

   protected:
    int num_features_ = -1;
    VecT max_X_;
    LabelT max_y_ = 0;
};

}  // namespace ml
}  // namespace lib
}  // namespace husky
