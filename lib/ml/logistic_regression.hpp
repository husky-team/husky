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

#include "core/objlist.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/ml/feature_label.hpp"
#include "lib/ml/parameter.hpp"
#include "lib/ml/regression.hpp"

namespace husky {
namespace lib {
namespace ml {

template <typename FeatureT, typename LabelT, bool is_sparse, typename ParamT>
class LogisticRegression : public Regression<FeatureT, LabelT, is_sparse, ParamT> {
   public:
    using ObjT = LabeledPointHObj<FeatureT, LabelT, is_sparse>;
    using ObjL = ObjList<ObjT>;

    // constructors
    LogisticRegression() : Regression<FeatureT, LabelT, is_sparse, ParamT>() {}
    explicit LogisticRegression(int _num_feature) : Regression<FeatureT, LabelT, is_sparse, ParamT>(_num_feature + 1) {
        this->num_feature_ = _num_feature;

        // gradient function: X * (true_y - innerproduct(xv, pv))
        this->gradient_func_ = [](ObjT& this_obj, Vector<FeatureT, false>& param) {
            auto vec_X = this_obj.x;
            auto pred_y = param.dot_with_intcpt(vec_X);
            pred_y = static_cast<FeatureT>(1. / (1. + exp(-pred_y)));
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
            pred_y += param_list.param_at(param_list.get_num_param() - 1);
            pred_y = (pred_y > 0) ? 1 : 0;
            return (pred_y == this_obj.y) ? 0 : 1;
        };

        // prediction function
        this->predict_func_ = [](ObjT& this_obj, ParamT& param_list) -> LabelT {
            const auto& vec_X = this_obj.x;
            FeatureT pred_y = 0;
            for (auto it = vec_X.begin_feaval(); it != vec_X.end_feaval(); ++it) {
                const auto& x = *it;
                pred_y += param_list.param_at(x.fea) * x.val;
            }
            pred_y += param_list.param_at(param_list.get_num_param() - 1);  // intercept
            this_obj.y = (pred_y > 0) ? 1 : 0;
            return this_obj.y;
        };
    }

    void set_num_feature(int _n_feature) {
        this->set_num_param(_n_feature + 1);
        this->num_feature_ = _n_feature;
    }

    int get_num_feature() { return this->num_feature_; }

   protected:
    LabelT predict_y(ObjT& this_obj) {
        const auto& vec_X = this_obj.x;
        FeatureT pred_y = 0;
        for (auto it = vec_X.begin_feaval(); it != vec_X.end_feaval(); ++it) {
            const auto& x = *it;
            pred_y += this->param_list_.param_at(x.fea) * x.val;
        }
        pred_y += this->param_list_.param_at(this->num_feature_);  // intercept
        return static_cast<LabelT>((pred_y > 0) ? 1 : 0);
    }
};  // LogisticRegression

}  // namespace ml
}  // namespace lib
}  // namespace husky
