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
#include <functional>

#include "core/context.hpp"
#include "core/objlist.hpp"
#include "core/utils.hpp"
#include "lib/aggregator.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/ml/feature_label.hpp"
#include "lib/ml/parameter.hpp"
#include "lib/vector.hpp"

namespace husky {
namespace lib {
namespace ml {

using husky::lib::AggregatorFactory;
using husky::lib::Aggregator;

// base class for regression
template <typename FeatureT, typename LabelT, bool is_sparse, typename ParamT = ParameterBucket<double>>
class Regression {
    typedef LabeledPointHObj<FeatureT, LabelT, is_sparse> ObjT;
    typedef ObjList<ObjT> ObjL;

   public:
    bool report_per_round = false;  // whether report error per iteration

    // constructors
    Regression() {}
    explicit Regression(int _num_param) { set_num_param(_num_param); }
    Regression(  // standard (3 args):
        std::function<Vector<FeatureT, is_sparse>(ObjT&, Vector<FeatureT, false>&)> _gradient_func,  // gradient func
        std::function<FeatureT(ObjT&, ParamT&)> _error_func,                                         // error function
        int _num_param)                                                                              // number of params
        : gradient_func_(_gradient_func),
          error_func_(_error_func) {
        set_num_param(_num_param);
    }

    // initialize parameter with positive size
    void set_num_param(int _num_param) {
        if (_num_param > 0) {
            param_list_.init(_num_param, 0.0);
        }
    }

    // query parameters
    int get_num_param() { return param_list_.get_num_param(); }  // get parameter size
    void present_param() {                                       // print each parameter to log
        if (this->trained_ == true)
            param_list_.present();
    }

    // predict and store in y
    void set_predict_func(std::function<LabelT(ObjT&, ParamT&)> _predict_func) { this->predict_func_ = _predict_func; }
    void predict(ObjL& data) {
        ASSERT_MSG(this->predict_func_ != nullptr, "Predict function is not specified.");
        list_execute(data, [&, this](ObjT& this_obj) {
            auto& y = this_obj.y;
            y = this->predict_func_(this_obj, this->param_list_);
        });
    }

    // calculate average error rate
    FeatureT avg_error(ObjL& data) {
        Aggregator<int> num_samples_agg(0, [](int& a, const int& b) { a += b; });
        Aggregator<FeatureT> error_agg(0.0, [](FeatureT& a, const FeatureT& b) { a += b; });
        auto& ac = AggregatorFactory::get_channel();
        list_execute(data, {}, {&ac}, [&, this](ObjT& this_obj) {
            error_agg.update(this->error_func_(this_obj, this->param_list_));
            num_samples_agg.update(1);
        });
        int num_samples = num_samples_agg.get_value();
        auto global_error = error_agg.get_value();
        auto mse = global_error / num_samples;
        return mse;
    }

    // train model
    template <template <typename, typename, bool> typename GD>
    void train(ObjL& data, int iters, double learning_rate) {
        // check conditions
        ASSERT_MSG(param_list_.get_num_param() > 0, "The number of parameters is 0.");
        ASSERT_MSG(gradient_func_ != nullptr, "Gradient function is not specified.");
        ASSERT_MSG(error_func_ != nullptr, "Error function is not specified.");

        // statistics: total number of samples and error
        Aggregator<int> num_samples_agg(0, [](int& a, const int& b) { a += b; });            // total number of samples
        Aggregator<FeatureT> error_stat(0.0, [](FeatureT& a, const double& b) { a += b; });  // sum of error
        error_stat.to_reset_each_iter();                                                     // reset each round

        // get statistics
        auto& ac = AggregatorFactory::get_channel();
        list_execute(data, {}, {&ac}, [&](ObjT& this_obj) { num_samples_agg.update(1); });
        int num_samples = num_samples_agg.get_value();
        // report statistics
        if (Context::get_global_tid() == 0) {
            husky::base::log_msg("Training set size = " + std::to_string(num_samples));
        }

        // use gradient descent to calculate step
        GD<FeatureT, LabelT, is_sparse> gd(gradient_func_, learning_rate);

        for (int round = 0; round < iters; round++) {
            // delegate update operation to gd
            gd.update_vec(data, param_list_, num_samples);

            // option to report error rate
            if (this->report_per_round == true) {
                // calculate error rate
                list_execute(data, {}, {&ac}, [&, this](ObjT& this_obj) {
                    auto error = this->error_func_(this_obj, this->param_list_);
                    error_stat.update(error);
                });
                if (Context::get_global_tid() == 0) {
                    base::log_msg("The error in iteration " + std::to_string(round + 1) + ": " +
                                  std::to_string(error_stat.get_value() / num_samples));
                }
            }
        }
        this->trained_ = true;
    }  // end of train

    // train and test model with early stopping
    template <template <typename, typename, bool> typename GD>
    void train_test(ObjL& data, ObjL& Test, int iters, double learning_rate) {
        // check conditions
        ASSERT_MSG(param_list_.get_num_param() > 0, "The number of parameters is 0.");
        ASSERT_MSG(gradient_func_ != nullptr, "Gradient function is not specified.");
        ASSERT_MSG(error_func_ != nullptr, "Error function is not specified.");

        // statistics: total number of samples and error
        Aggregator<int> num_samples_agg(0, [](int& a, const int& b) { a += b; });  // total number of samples
        Aggregator<FeatureT> error_stat(0.0, [](FeatureT& a, const FeatureT& b) { a += b; });  // sum of error
        error_stat.to_reset_each_iter();                                                       // reset each round

        // get statistics
        auto& ac = AggregatorFactory::get_channel();
        list_execute(data, {}, {&ac}, [&](ObjT& this_obj) { num_samples_agg.update(1); });
        int num_samples = num_samples_agg.get_value();
        // report statistics
        if (Context::get_global_tid() == 0) {
            husky::base::log_msg("Training set size = " + std::to_string(num_samples));
        }

        // use gradient descent to calculate step
        GD<FeatureT, LabelT, is_sparse> gd(gradient_func_, learning_rate);
        double pastError = 0.0;

        for (int round = 0; round < iters; round++) {
            // delegate update operation to gd
            gd.update_vec(data, param_list_, num_samples);

            auto currentError = avg_error(Test);
            // option to report error rate
            if (this->report_per_round == true) {
                if (Context::get_global_tid() == 0) {
                    base::log_msg("The error in iteration " + std::to_string(round + 1) + ": " +
                                  std::to_string(currentError));
                }
            }

            // TODO(Tatiana): handle fluctuation in test error
            // validation based early stopping -- naive version
            if (currentError == 0.0 || (round != 0 && currentError > pastError)) {
                if (Context::get_global_tid() == 0) {
                    base::log_msg("Early stopping invoked. Training is completed.");
                }
                break;
            }
            pastError = currentError;
        }

        this->trained_ = true;
    }  // end of train_test

   protected:
    std::function<Vector<FeatureT, is_sparse>(ObjT&, Vector<FeatureT, false>&)> gradient_func_ = nullptr;
    std::function<FeatureT(ObjT&, ParamT&)> error_func_ = nullptr;  // error function
    std::function<LabelT(ObjT&, ParamT&)> predict_func_ = nullptr;
    ParamT param_list_;     // parameter vector list
    int num_feature_ = -1;  // number of features (maybe != num_param)
    bool trained_ = false;  // indicate if model is trained
};                          // Regression

}  // namespace ml
}  // namespace lib
}  // namespace husky
