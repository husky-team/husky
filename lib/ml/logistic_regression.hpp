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
#include <utility>
#include <vector>

#include "core/engine.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/ml/feature_label.hpp"
#include "lib/ml/gradient_descent.hpp"
#include "lib/ml/parameter.hpp"
#include "lib/ml/regression.hpp"
#include "lib/ml/vector_linalg.hpp"

namespace husky {
namespace lib {
namespace ml {

using vec_double = std::vector<double>;
using husky::lib::AggregatorFactory;
using husky::lib::Aggregator;

template <typename ObjT = FeatureLabel, typename ParamT = ParameterBucket<double>>
class LogisticRegression : public Regression<ObjT, ParamT> {
    using ObjL = ObjList<ObjT>;

   public:
    // constructors
    LogisticRegression() : Regression<ObjT, ParamT>() {}
    explicit LogisticRegression(int _num_feature) { this->set_num_param(_num_feature + 1); }
    LogisticRegression(
        std::function<std::vector<std::pair<int, double>>(ObjT&)> _getX,  // function to get feature vector
        std::function<double(ObjT&)> _gety,                               // function to get label
        int _num_feature                                                  // number of features
        )
        : Regression<ObjT, ParamT>(_num_feature + 1) {
        // gradient function: X * (true_y - innerproduct(xv, pv))
        this->gradient_func_ = [this](ObjT& this_obj, std::function<double(int)> param_at) {
            std::vector<std::pair<int, double>> vec_X = this->get_X_(this_obj);
            vec_X.push_back(std::make_pair(0, 1.0));  // intercept factor
            double pred_y = 0;

            for (auto& x : vec_X) {
                pred_y += param_at(x.first) * x.second;
            }

            pred_y = 1. / (1. + exp(-pred_y));
            double delta = this->get_y_(this_obj) - pred_y;
            for (auto& w : vec_X) {
                w.second *= delta;
            }

            return vec_X;  // gradient vector
        };

        // error function: (true_y - pred_y)^2
        this->error_func_ = [this](ObjT& this_obj, ParamT& param_list) {
            std::vector<std::pair<int, double>> vec_X = this->get_X_(this_obj);
            vec_X.push_back(std::make_pair(0, 1.0));  // intercept factor
            double pred_y = 0;
            for (auto& x : vec_X) {
                pred_y += param_list.param_at(x.first) * x.second;
            }
            pred_y = (pred_y > 0) ? 1 : 0;
            return (pred_y == this->get_y_(this_obj)) ? 0 : 1;
        };

        this->num_feature_ = _num_feature;
        this->get_X_ = _getX;
        this->get_y_ = _gety;
    }

    void set_num_feature(int _n_feature) {
        this->set_num_param(_n_feature + 1);  // intercept term
        this->num_feature_ = _n_feature;
    }

    int get_num_feature() { return this->num_feature_; }

   protected:
    std::function<std::vector<std::pair<int, double>>(ObjT&)> get_X_ = nullptr;  // get feature vector (idx,val)
    std::function<double(ObjT&)> get_y_ = nullptr;                               // get label

    double predict_y(ObjT& this_obj) {
        std::vector<std::pair<int, double>> vec_X = get_X_(this_obj);
        double pred_y = 0;
        for (auto& x : vec_X)
            pred_y += this->param_list_.param_at(x.first) * x.second;
        pred_y += this->param_list_.param_at(this->num_feature_) * 1.0;  // intercept factor
        return 1. / (1. + exp(-pred_y));
    }

    double delta_func(ObjT& this_obj) { return this->get_y_(this_obj) - predict_y(this_obj); }
};  // LogisticRegression

template <>
LogisticRegression<SparseFeatureLabel, ParameterBucket<double>>::LogisticRegression(int _num_feature)
    : LogisticRegression([](SparseFeatureLabel& this_obj) { return this_obj.get_feature(); },
                         [](SparseFeatureLabel& this_obj) { return (this_obj.get_label() > 0) ? 1 : 0; },
                         _num_feature) {}

}  // namespace ml
}  // namespace lib
}  // namespace husky
