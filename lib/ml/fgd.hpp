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

#include <cmath>
#include <functional>
#include <utility>
#include <vector>

#include "core/engine.hpp"
#include "lib/ml/feature_label.hpp"
#include "lib/ml/gradient_descent.hpp"
#include "lib/ml/parameter.hpp"
#include "lib/ml/vector_linalg.hpp"

namespace husky {
namespace lib {
namespace ml {

using lib::AggregatorFactory;
using vec_double = std::vector<double>;
using vec_sp = std::vector<std::pair<int, double>>;

// base class for full(batch) gradient descent, extends GradientDescentBase
template <typename ObjT = FeatureLabel, typename ParamT = ParameterBucket<double>>
class FGD : public GradientDescentBase<ObjT> {
   private:
    using ObjL = ObjList<ObjT>;

   public:
    // constructors
    FGD() : GradientDescentBase<ObjT>() {}
    explicit FGD(std::function<vec_sp(ObjT&, std::function<double(int)>)> _gradient_func, double _learning_rate)
        : GradientDescentBase<ObjT>(_gradient_func, _learning_rate) {}

    void update_vec(ObjL& data, ParamT param_list, int num_global_samples) override {
        ASSERT_MSG(this->learning_rate_ != 0, "Learning rate is set to 0.");
        ASSERT_MSG(this->gradient_func_ != nullptr, "Gradient function is not specified.");

        vec_double current_vec = param_list.get_all_param();  // local copy of parameter
        auto& ac = AggregatorFactory::get_channel();
        list_execute(data, {}, {&ac}, [&grad_func = this->gradient_func_, &rate = this->learning_rate_,
                            &current_vec, &param_list, &num_global_samples](ObjT& this_obj) {
            vec_sp grad = grad_func(this_obj,
                            [&current_vec](int idx){ return current_vec[idx]; });  // calculate gradient
            for (auto& w : grad) {
                double delta = w.second * rate;
                param_list.update(w.first, delta / num_global_samples);
            }
        });
    }
};

}  // namespace ml
}  // namespace lib
}  // namespace husky
