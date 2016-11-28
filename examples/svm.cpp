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

//
// Parameters
//
// lambda
// type: double
// info: regularization parameter
//
// train
// type: string
// info: the path of training data in hadoop, in LibSVM format
//
// test
// type: string
// info: the path of testing data in hadoop, in LibSVM format
//
// n_iter
// type: int
// info: number of epochs the entire training data will be went through
//
// is_sparse
// type: string
// info: whether the data is dense or sparse
//
// format
// type: string
// info: the data format of input file: libsvm/tsv
//
// configuration example:
// train=hdfs:///datasets/classification/a9
// test=hdfs:///datasets/classification/a9t
// is_sparse=true
// format=libsvm
// n_iter=50
// lambda=0.01

#include <algorithm>
#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "lib/ml/data_loader.hpp"
#include "lib/ml/feature_label.hpp"
#include "lib/ml/parameter.hpp"

using husky::lib::Aggregator;
using husky::lib::AggregatorFactory;

template<bool is_sparse>
void svm() {
    using ObjT = husky::lib::ml::LabeledPointHObj<double, double, is_sparse>;
    auto& train_set = husky::ObjListStore::create_objlist<ObjT>("train_set");
    auto& test_set = husky::ObjListStore::create_objlist<ObjT>("test_set");

    auto format_str = husky::Context::get_param("format");
    husky::lib::ml::DataFormat format;
    if (format_str == "libsvm") {
        format = husky::lib::ml::kLIBSVMFormat;
    } else if (format_str == "tsv") {
        format = husky::lib::ml::kTSVFormat;
    }

    // load data
    int num_features = husky::lib::ml::load_data(husky::Context::get_param("train"), train_set, format);
    husky::lib::ml::load_data(husky::Context::get_param("test"), test_set, format);

    // get model config parameters
    double lambda = std::stod(husky::Context::get_param("lambda"));
    int num_iter = std::stoi(husky::Context::get_param("n_iter"));

    // initialize parameters
    husky::lib::ml::ParameterBucket<double> param_list(num_features + 1);  // scalar b and vector w

    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("num of params: " + std::to_string(param_list.get_num_param()));
    }

    // get the number of global records
    Aggregator<int> num_samples_agg(0, [](int& a, const int& b) { a += b; });
    num_samples_agg.update(train_set.get_size());
    AggregatorFactory::sync();
    int num_samples = num_samples_agg.get_value();
    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Training set size = " + std::to_string(num_samples));
    }

    // Aggregators for regulator, w square and loss
    Aggregator<double> regulator_agg(0.0, [](double& a, const double& b) { a += b; });
    Aggregator<double> sqr_w_agg(0.0, [](double& a, const double& b) { a += b; });
    sqr_w_agg.to_reset_each_iter();
    Aggregator<double> loss_agg(0.0, [](double& a, const double& b) { a += b; });
    loss_agg.to_reset_each_iter();

    // Main loop
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < num_iter; i++) {
        double sqr_w = 0.0;      // ||w||^2
        double regulator = 0.0;  // prevent overfitting

        // calculate w square
        for (int idx = 0; idx < num_features; idx++) {
            double w = param_list.param_at(idx);
            sqr_w += w * w;
        }

        // get local copy of parameters
        auto bweight = param_list.get_all_param();

        // calculate regulator
        regulator = (sqr_w == 0) ? 1.0 : std::min(1.0, 1.0 / sqrt(sqr_w * lambda));
        if (regulator < 1) {
            bweight *= regulator;
            sqr_w = 1 / lambda;
        }

        double eta = 1.0 / (i+1);

        // regularize w in param_list
        if (husky::Context::get_global_tid() == 0) {
            for (int idx = 0; idx < num_features; idx++) {
                double w = bweight[idx];
                param_list.update(idx, (w - w / regulator - eta * w));
            }
        }

        auto& ac = AggregatorFactory::get_channel();
        // calculate gradient
        list_execute(train_set, {}, {&ac}, [&](ObjT& this_obj) {
           double prod = 0;  // prod = WX * y
           double y = this_obj.y;
           auto X = this_obj.x;
           for (auto it = X.begin_feaval(); it != X.end_feaval(); ++it)
               prod += bweight[(*it).fea] * (*it).val;
           // bias
           prod += bweight[num_features];
           prod *= y;

           if (prod < 1) {  // the data point falls within the margin
               for (auto it = X.begin_feaval(); it != X.end_feaval(); ++it) {
                   auto x = *it;
                   x.val *= y;  // calculate the gradient for each parameter
                   param_list.update(x.fea, eta * x.val / num_samples / lambda);
               }
               // update bias
               param_list.update(num_features, eta * y / num_samples);
               loss_agg.update(1 - prod);
           }
           sqr_w_agg.update(sqr_w);
           regulator_agg.update(regulator);
        });

        int num_samples = num_samples_agg.get_value();
        sqr_w = sqr_w_agg.get_value() / num_samples;
        regulator = regulator_agg.get_value() / num_samples;
        double loss = lambda / 2 * sqr_w + loss_agg.get_value() / num_samples;
        if (husky::Context::get_global_tid() == 0) {
            husky::base::log_msg("Iteration " + std::to_string(i+1)
                    + ": ||w|| = " + std::to_string(sqrt(sqr_w))
                    + ", loss = " + std::to_string(loss));
        }
    }
    auto end = std::chrono::steady_clock::now();

    // Show result
    if (husky::Context::get_global_tid() == 0) {
        param_list.present();
        husky::base::log_msg("Time per iter: " +
            std::to_string(std::chrono::duration_cast<std::chrono::duration<float>>(end - start).count() / num_iter));
    }

    // test
    Aggregator<int> error_agg(0, [](int& a, const int& b) { a += b; });
    Aggregator<int> num_test_agg(0, [](int& a, const int& b) { a += b; });
    auto& ac = AggregatorFactory::get_channel();
    auto bweight = param_list.get_all_param();
    list_execute(test_set, {}, {&ac}, [&](ObjT& this_obj) {
       double indicator = 0;
       auto y = this_obj.y;
       auto X = this_obj.x;
       for (auto it = X.begin_feaval(); it != X.end_feaval(); it++)
           indicator += bweight[(*it).fea] * (*it).val;
       // bias
       indicator += bweight[num_features];
       indicator *= y;  // right prediction if positive (Wx+b and y have the same sign)
       if (indicator < 0) error_agg.update(1);
       num_test_agg.update(1);
    });

    if (husky::Context::get_global_tid() == 0) {
        husky::base::log_msg("Error rate on testing set: "
                + std::to_string(static_cast<double>(error_agg.get_value()) / num_test_agg.get_value()));
    }
}

void init() {
    if (husky::Context::get_param("is_sparse") == "true") {
        svm<true>();
    } else {
        svm<false>();
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args({
        "hdfs_namenode", "hdfs_namenode_port", "train", "test", "n_iter", "lambda", "format", "is_sparse"
    });
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(init);
        return 0;
    }
    return 1;
}
