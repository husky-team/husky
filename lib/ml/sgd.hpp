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
#include "core/utils.hpp"
#include "lib/ml/feature_label.hpp"
#include "lib/ml/gradient_descent.hpp"
#include "lib/ml/parameter.hpp"
#include "lib/vector.hpp"

namespace husky {
namespace lib {
namespace ml {

// base class for stochastic gradient descent, extends GradientDescentBase
template <typename FeatureT, typename LabelT, bool is_sparse>
class SGD : public GradientDescentBase<FeatureT, LabelT, is_sparse> {
   private:
    using ObjT = LabeledPointHObj<FeatureT, LabelT, is_sparse>;
    using ObjL = ObjList<ObjT>;
    using VecT = Vector<FeatureT, is_sparse>;

   public:
    // constructors
    SGD() : GradientDescentBase<FeatureT, LabelT, is_sparse>() {}
    SGD(std::function<VecT(ObjT&, Vector<FeatureT, false>&)> _gradient_func, double _learning_rate)
        : GradientDescentBase<FeatureT, LabelT, is_sparse>(_gradient_func, _learning_rate) {}

    template <typename ParamT>
    void update_vec(ObjL& data, ParamT& param_list, int num_global_samples) {
        ASSERT_MSG(this->learning_rate_ != 0, "Learning rate is set to 0.");
        ASSERT_MSG(this->gradient_func_ != nullptr, "Gradient function is not specified.");

        int num_local_samples = data.get_size();        // number of local samples
        auto current_vec = param_list.get_all_param();  // local copy of parameter
        auto& ac = AggregatorFactory::get_channel();

        list_execute(data, {}, {&ac}, [&, this](ObjT& this_obj) {
            auto grad = this->gradient_func_(this_obj, current_vec);  // calculate gradient
            for (auto it = grad.begin_feaval(); it != grad.end_feaval(); ++it) {
                const auto& w = *it;
                auto delta = w.val * this->learning_rate_;
                current_vec[w.fea] += delta;
                param_list.update(w.fea, delta * num_local_samples / num_global_samples);
            }
        });
    }
};

}  // namespace ml
}  // namespace lib
}  // namespace husky
