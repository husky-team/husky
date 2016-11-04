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
#include "lib/ml/parameter.hpp"
#include "lib/ml/vector_linalg.hpp"

namespace husky {
namespace lib {
namespace ml {

using lib::AggregatorFactory;
using vec_double = std::vector<double>;
using vec_sp = std::vector<std::pair<int, double>>;

// base class for gradient descent
template <typename ObjT = FeatureLabel, typename ParamT = ParameterBucket<double>>
class GradientDescentBase {
   private:
    using ObjL = ObjList<ObjT>;

   public:
    // constructors
    GradientDescentBase() = default;
    GradientDescentBase(  // standard constructor accepting 3 arguments:
        std::function<vec_sp(ObjT&, std::function<double(int)>)> _gradient_func,  // function to calculate gradient
        double _learning_rate                                                     // learning rate
        )
        : gradient_func_(_gradient_func), learning_rate_(_learning_rate) {}

    // get update vector for parameters
    virtual void update_vec(ObjL& data, ParamT param_list, int num_global_samples) = 0;

    // manipulate gradient function and learning rate
    void set_gradient_func(std::function<vec_sp(ObjT&, std::function<double(int)>)> _gradient_func) {
        this->gradient_func_ = _gradient_func;
    }
    void set_learning_rate(double _learning_rate) { this->learning_rate_ = _learning_rate; }

   protected:
    std::function<vec_sp(ObjT&, std::function<double(int)>)> gradient_func_ =
        nullptr;  // function to calculate gradient
    double learning_rate_;
};  // GradientDescentBase

}  // namespace ml
}  // namespace lib
}  // namespace husky
