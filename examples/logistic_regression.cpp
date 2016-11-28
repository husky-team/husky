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
// alpha
// type: double
// info: (a.k.a learning rate)
//       The step size of gradient descent. You may use 0.01 if not sure .
//
// train
// type: string
// info: The path of training data in hadoop
//
// test
// type: string
// info: The path of testing data in hadoop
//
// is_sparse
// type: string
// info: whether the data is dense or sparse
//
// format
// type: string
// info: the data format of input file: libsvm/tsv
//
// n_iter
// type: int
// info: (a.k.a number of epoch)
//       How many time the entire training data will be went through
//
// Configuration example:
// train=hdfs:///datasets/classification/a9
// test=hdfs:///datasets/classification/a9t
// is_sparse=true
// format=libsvm
// n_iter=50
// alpha=0.5

#include <string>
#include <vector>

#include "core/engine.hpp"
#include "lib/ml/data_loader.hpp"
#include "lib/ml/fgd.hpp"
#include "lib/ml/logistic_regression.hpp"

using husky::lib::ml::ParameterBucket;

template <bool is_sparse>
void logistic_regression() {
    using LabeledPointHObj = husky::lib::ml::LabeledPointHObj<double, double, is_sparse>;
    auto & train_set = husky::ObjListStore::create_objlist<LabeledPointHObj>("train_set");
    auto & test_set = husky::ObjListStore::create_objlist<LabeledPointHObj>("test_set");

    // load data
    auto format_str = husky::Context::get_param("format");
    husky::lib::ml::DataFormat format;
    if (format_str == "libsvm") {
        format = husky::lib::ml::kLIBSVMFormat;
    } else if (format_str == "tsv") {
        format = husky::lib::ml::kTSVFormat;
    }

    int num_features = husky::lib::ml::load_data(husky::Context::get_param("train"), train_set, format);
    husky::lib::ml::load_data(husky::Context::get_param("test"), test_set, format, num_features);

    // processing labels
    husky::list_execute(train_set, [](auto& this_obj) {
        if (this_obj.y < 0) this_obj.y = 0;
    });
    husky::list_execute(test_set, [](auto& this_obj) {
        if (this_obj.y < 0) this_obj.y = 0;
    });

    double alpha = std::stod(husky::Context::get_param("alpha"));
    int num_iter = std::stoi(husky::Context::get_param("n_iter"));

    // initialize logistic regression model
    husky::lib::ml::LogisticRegression<double, double, is_sparse, ParameterBucket<double>> lr(num_features);
    lr.report_per_round = true;  // report training error per round

    // train the model
    lr.template train<husky::lib::ml::FGD>(train_set, num_iter, alpha);

    // estimate generalization error
    auto test_error = lr.avg_error(test_set);
    if (husky::Context::get_global_tid() == 0) {
        // validation
        husky::base::log_msg("Error on testing set: " + std::to_string(test_error));
    }
}

void init() {
    if (husky::Context::get_param("is_sparse") == "true") {
        logistic_regression<true>();
    } else {
        logistic_regression<false>();
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args(8);
    args[0] = "hdfs_namenode";
    args[1] = "hdfs_namenode_port";
    args[2] = "train";
    args[3] = "test";
    args[4] = "n_iter";
    args[5] = "alpha";
    args[6] = "format";
    args[7] = "is_sparse";
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(init);
        return 0;
    }
    return 1;
}
