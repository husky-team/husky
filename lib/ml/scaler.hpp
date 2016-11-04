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

#include "lib/ml/vector_linalg.hpp"

namespace husky {
namespace lib {
namespace ml {

typedef std::vector<double> vec_double;
typedef std::vector<std::pair<int, double>> vec_sp;

// get max absolute value
void set_max(vec_double& va, const vec_sp& vb) {
    int n = vb.size();
    for (int i = 0; i < n; i++) {
        va[vb[i].first] = std::max(va[vb[i].first], std::abs(vb[i].second));
    }
}
void set_max(vec_double& va, const vec_double& vb) {
    int n = vb.size();
    for (int i = 0; i < n; i++)
        va[i] = std::max(va[i], std::abs(vb[i]));
}

// scaling each element in vector to [-1, 1]
template <typename FeatureT = vec_sp, typename LabelT = double,
          template <typename, typename> typename DataT = FeatureLabelBase>
class LinearScaler {
    typedef DataT<FeatureT, LabelT> ObjT;
    typedef ObjList<ObjT> ObjL;

   public:
    explicit LinearScaler(int);
    LinearScaler(std::function<FeatureT&(ObjT&)> _use_X, std::function<LabelT&(ObjT&)> _use_y, int _num_features)
        : use_X_(_use_X), use_y_(_use_y), num_features_(_num_features) {
        if (_num_features > 0)
            max_X_ = vec_double(_num_features + 1, 0.0);
    }

    void fit(ObjL& data) {
        ASSERT_MSG(num_features_ > 0, "Non-positive number of parameters.");
        lib::Aggregator<vec_double> max_X_agg(
            max_X_, [](vec_double& va, const vec_double& vb) { set_max(va, vb); },
            [this](vec_double& va) { va = vec_double(this->num_features_ + 1, 0.0); });
        lib::Aggregator<double> max_y_agg(max_y_, [](double& a, const double& b) { a = std::max(a, std::abs(b)); });
        auto& ch = lib::AggregatorFactory::get_channel();

        list_execute(data, {}, {&ch}, [&](ObjT& this_obj) {
            FeatureT& X = use_X_(this_obj);
            max_X_agg.update_any([&, this](vec_double& va) { set_max(va, this->use_X_(this_obj)); });
            max_y_agg.update(use_y_(this_obj));
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
            FeatureT& X = use_X_(this_obj);
            LabelT& y = use_y_(this_obj);
            X /= max_X_;
            if (max_y_ > 0)
                y /= max_y_;
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
            FeatureT& X = use_X_(this_obj);
            LabelT& y = use_y_(this_obj);
            X *= max_X_;
            if (max_y_ > 0)
                y *= max_y_;
        });
    }

    template <typename... Args>
    void inverse_transform(ObjL& data, Args&... args) {
        inverse_transform(data);
        inverse_transform(args...);
    }

    vec_double get_max_X() { return this->max_X_; }
    double get_max_y() { return this->max_y_; }

   protected:
    int num_features_ = -1;
    vec_double max_X_;
    double max_y_ = 0;
    std::function<FeatureT&(ObjT&)> use_X_ = nullptr;
    std::function<LabelT&(ObjT&)> use_y_ = nullptr;
};

template <>
LinearScaler<vec_sp, double, FeatureLabelBase>::LinearScaler(int _num_features)
    : LinearScaler([](FeatureLabelBase<vec_sp, double>& this_obj) -> vec_sp& { return this_obj.use_feature(); },
                   [](FeatureLabelBase<vec_sp, double>& this_obj) -> double& { return this_obj.use_label(); },
                   _num_features) {}

}  // namespace ml
}  // namespace lib
}  // namespace husky
