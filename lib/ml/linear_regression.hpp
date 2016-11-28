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

#include "core/executor.hpp"
#include "core/objlist.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/ml/feature_label.hpp"
#include "lib/ml/parameter.hpp"
#include "lib/ml/regression.hpp"

namespace husky {
namespace lib {
namespace ml {

template <typename FeatureT, typename LabelT, bool is_sparse, typename ParamT>
class LinearRegression : public Regression<FeatureT, LabelT, is_sparse, ParamT> {
   public:
    using ObjT = LabeledPointHObj<FeatureT, LabelT, is_sparse>;
    using ObjL = ObjList<ObjT>;

    // constructors
    LinearRegression() : Regression<FeatureT, LabelT, is_sparse, ParamT>() {}
    explicit LinearRegression(int _num_feature) : Regression<FeatureT, LabelT, is_sparse, ParamT>(_num_feature + 1) {
        this->num_feature_ = _num_feature;

        // gradient function: X * (true_y - innerproduct(xv, pv))
        this->gradient_func_ = [](ObjT& this_obj, Vector<FeatureT, false>& param) {
            auto vec_X = this_obj.x;
            auto pred_y = param.dot_with_intcpt(vec_X);
            auto delta = static_cast<FeatureT>(this_obj.y) - pred_y;
            vec_X *= delta;
            int num_param = param.get_feature_num();
            vec_X.resize(num_param);
            vec_X.set(num_param - 1, delta);  // intercept factor
            return vec_X;                     // gradient vector
        };

        // error function: (true_y - pred_y)^2
        this->error_func_ = [](ObjT& this_obj, ParamT& param_list) {
            const auto& vec_X = this_obj.x;
            FeatureT pred_y = 0;
            for (auto it = vec_X.begin_feaval(); it != vec_X.end_feaval(); ++it) {
                const auto& x = *it;
                pred_y += param_list.param_at(x.fea) * x.val;
            }
            pred_y += param_list.param_at(param_list.get_num_param() - 1);  // intercept factor
            auto delta = static_cast<FeatureT>(this_obj.y) - pred_y;
            return delta * delta;
        };

        // predict function
        this->predict_func_ = [](ObjT& this_obj, ParamT& param_list) -> LabelT {
            auto vec_X = this_obj.x;
            this_obj.y = 0;
            for (auto it = vec_X.begin_feaval(); it != vec_X.end_feaval(); it++) {
                const auto& x = *it;
                this_obj.y += param_list.param_at(x.fea) * x.val;
            }
            this_obj.y += param_list.param_at(param_list.get_num_param() - 1);  // intercept factor
            return this_obj.y;
        };
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
        list_execute(data, {}, {&ch}, [&](ObjT& this_obj) {
            num_samples_agg.update(1);                          // number of records
            sum_y_agg.update(static_cast<double>(this_obj.y));  // sum of y
        });
        double y_mean = sum_y_agg.get_value() / num_samples_agg.get_value();  // y mean

        list_execute(data, {}, {&ch}, [&, this](ObjT& this_obj) {
            auto y_true = this_obj.y;
            auto y_pred = this->predict_y(this_obj);
            double error = static_cast<double>(y_true - y_pred);
            double variance = static_cast<double>(y_true - y_mean);
            sum_error_agg.update(error * error);
            sum_var_agg.update(variance * variance);
        });
        double score = 1 - sum_error_agg.get_value() / sum_var_agg.get_value();
        return score;
    }

   protected:
    LabelT predict_y(ObjT& this_obj) {
        auto vec_X = this_obj.x;
        LabelT pred_y = 0;
        for (auto it = vec_X.begin_feaval(); it != vec_X.end_feaval(); it++) {
            const auto& x = *it;
            pred_y += this->param_list_.param_at(x.fea) * x.val;
        }
        pred_y += this->param_list_.param_at(this->num_feature_);  // intercept factor
        return pred_y;
    }

    LabelT delta_func(ObjT& this_obj) { return this_obj.y - predict_y(this_obj); }
};

}  // namespace ml
}  // namespace lib
}  // namespace husky
