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
class LinearRegression : public Regression<ObjT, ParamT> {
    using ObjL = ObjList<ObjT>;

   public:
    // constructors
    LinearRegression() : Regression<ObjT, ParamT>() {}
    explicit LinearRegression(int _num_feature) : Regression<ObjT, ParamT>(_num_feature + 1) {}
    LinearRegression(                                                     // standard constructor accepting 3 arguments:
        std::function<std::vector<std::pair<int, double>>(ObjT&)> _getX,  // get feature vector (idx,val)
        std::function<double(ObjT&)> _gety,                               // get label
        int _num_feature                                                  // number of features
        )
        : Regression<ObjT, ParamT>(_num_feature + 1) {  // add intercept parameter
        // gradient function: X * (true_y - innerproduct(xv, pv))
        this->gradient_func_ = [this](ObjT& this_obj, std::function<double(int)> param_at) {
            std::vector<std::pair<int, double>> vec_X = this->get_X_(this_obj);
            vec_X.push_back(std::make_pair(0, 1.0));
            double pred_y = 0;
            for (auto& x : vec_X) {
                pred_y += param_at(x.first) * x.second;
            }
            double delta = this->get_y_(this_obj) - pred_y;
            for (auto& w : vec_X) {
                w.second *= delta;
            }

            return vec_X;  // gradient vector
        };

        // error function: (true_y - pred_y)^2
        this->error_func_ = [this](ObjT& this_obj, ParamT& param_list) {
            std::vector<std::pair<int, double>> vec_X = this->get_X_(this_obj);
            vec_X.push_back(std::make_pair(0, 1.0));
            double pred_y = 0;
            for (auto& x : vec_X) {
                pred_y += param_list.param_at(x.first) * x.second;
            }
            double delta = this->get_y_(this_obj) - pred_y;
            return delta * delta;
        };

        this->num_feature_ = _num_feature;
        this->get_X_ = _getX;
        this->get_y_ = _gety;
    }

    void set_num_feature(int _num_feature) {
        this->set_num_param(_num_feature + 1);  // intercept term
        this->num_feature_ = _num_feature;
    }

    int get_num_feature() { return this->num_feature_; }

    double score(ObjL& data) {
        auto sum_double = [](double& a, const double& b) { a += b; };
        auto sum_int = [](int& a, const int& b) { a += b; };

        Aggregator<int> num_samples_agg(0, sum_int);
        Aggregator<double> sum_y_agg(0.0, sum_double);
        Aggregator<double> sum_error_agg(0.0, sum_double);
        Aggregator<double> sum_var_agg(0.0, sum_double);

        auto& ch = AggregatorFactory::get_channel();
        list_execute(data, {}, {&ch}, [&, this](ObjT& this_obj) {
            num_samples_agg.update(1);                 // number of records
            sum_y_agg.update(this->get_y_(this_obj));  // sum of y
        });
        double y_mean = sum_y_agg.get_value() / num_samples_agg.get_value();  // y mean

        list_execute(data, {}, {&ch}, [&, this](ObjT& this_obj) {
            double y_true = this->get_y_(this_obj);
            double y_pred = this->predict_y(this_obj);
            double error = y_true - y_pred;
            double variance = y_true - y_mean;
            sum_error_agg.update(error * error);
            sum_var_agg.update(variance * variance);
        });
        double score = 1 - sum_error_agg.get_value() / sum_var_agg.get_value();
        return score;
    }

   protected:
    std::function<std::vector<std::pair<int, double>>(ObjT&)> get_X_ = nullptr;  // get feature vector (idx,val)
    std::function<double(ObjT&)> get_y_ = nullptr;                               // get label

    double predict_y(ObjT& this_obj) {
        std::vector<std::pair<int, double>> vec_X = get_X_(this_obj);
        double pred_y = 0;
        for (auto& x : vec_X) {
            pred_y += this->param_list_.param_at(x.first) * x.second;
        }
        pred_y += this->param_list_.param_at(this->num_feature_);  // intercept factor
        return pred_y;
    }

    double delta_func(ObjT& this_obj) { return this->get_y_(this_obj) - predict_y(this_obj); }
};

template <>
LinearRegression<SparseFeatureLabel, ParameterBucket<double>>::LinearRegression(int _num_feature)
    : LinearRegression([](SparseFeatureLabel& this_obj) { return this_obj.get_feature(); },
                       [](SparseFeatureLabel& this_obj) { return this_obj.get_label(); }, _num_feature) {}

}  // namespace ml
}  // namespace lib
}  // namespace husky
