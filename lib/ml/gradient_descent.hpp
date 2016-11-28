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
#include "lib/ml/feature_label.hpp"
#include "lib/ml/parameter.hpp"
#include "lib/vector.hpp"

namespace husky {
namespace lib {
namespace ml {

// base class for gradient descent
template <typename FeatureT, typename LabelT, bool is_sparse>
class GradientDescentBase {
   private:
    using ObjT = LabeledPointHObj<FeatureT, LabelT, is_sparse>;
    using ObjL = ObjList<ObjT>;
    using VecT = Vector<FeatureT, is_sparse>;

   public:
    // constructors
    GradientDescentBase() = default;
    GradientDescentBase(  // standard constructor accepting 2 arguments:
        std::function<VecT(ObjT&, Vector<FeatureT, false>&)> _gradient_func,  // gradient function
        double _learning_rate                                                 // learning rate
        )
        : gradient_func_(_gradient_func), learning_rate_(_learning_rate) {}

    // manipulate gradient function and learning rate
    inline void set_gradient_func(std::function<VecT(ObjT&, Vector<FeatureT, false>&)> _gradient_func) {
        this->gradient_func_ = _gradient_func;
    }
    inline void set_learning_rate(FeatureT _learning_rate) { this->learning_rate_ = _learning_rate; }

   protected:
    std::function<VecT(ObjT&, Vector<FeatureT, false>&)> gradient_func_ = nullptr;
    FeatureT learning_rate_;
};  // GradientDescentBase

}  // namespace ml
}  // namespace lib
}  // namespace husky
