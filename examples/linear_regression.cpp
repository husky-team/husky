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
// type: int
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
// n_iter
// type: int
// info: (a.k.a number of epoch)
//       How many time the entire training data will be went through
//
// is_sparse
// type: string
// info: whether the data is dense or sparse
//
// format
// type: string
// info: the data format of input file: libsvm/tsv
//
// Configuration example:
// train=hdfs:///datasets/regression/MSD/train
// test=hdfs:///datasets/regression/MSD/test
// is_sparse=false
// format=tsv
// n_iter=10
// alpha=0.1

#include <string>
#include <vector>

#include "core/engine.hpp"
#include "lib/ml/data_loader.hpp"
#include "lib/ml/linear_regression.hpp"
#include "lib/ml/scaler.hpp"
#include "lib/ml/sgd.hpp"

using husky::lib::ml::ParameterBucket;

void report(std::string msg) { if (husky::Context::get_global_tid() == 0) husky::base::log_msg(msg); }

template <bool is_sparse>
void linear_regression(double alpha, int num_iter, husky::lib::ml::DataFormat format) {
    typedef husky::lib::ml::LabeledPointHObj<double, double, is_sparse> LabeledPointHObj;
    auto & train_set = husky::ObjListStore::create_objlist<LabeledPointHObj>("train_set");
    auto & test_set = husky::ObjListStore::create_objlist<LabeledPointHObj>("test_set");

    // load data
    int num_features = husky::lib::ml::load_data(husky::Context::get_param("train"), train_set, format);
    husky::lib::ml::load_data(husky::Context::get_param("test"), test_set, format, num_features);

    // scale values to [-1, 1]
    // TODO(Tatiana): applying the same scaling results in large error?
    husky::lib::ml::LinearScaler<double, double, is_sparse> scaler_train(num_features);
    husky::lib::ml::LinearScaler<double, double, is_sparse> scaler_test(num_features);
    scaler_train.fit_transform(train_set);
    scaler_test.fit_transform(test_set);

    // initialize linear regression model
    husky::lib::ml::LinearRegression<double, double, is_sparse, ParameterBucket<double>> lr(num_features);

    lr.report_per_round = true;  // report training error per round
    lr.template train<husky::lib::ml::SGD>(train_set, num_iter, alpha);

    report("The error on training set = " + std::to_string(lr.avg_error(train_set)));
    report("The score on training set = " + std::to_string(lr.score(train_set)));
    // validation
    report("The error on testing set = " + std::to_string(lr.avg_error(test_set)));
    report("The score on testing set = " + std::to_string(lr.score(test_set)));
}

void initialize() {
    double alpha = std::stod(husky::Context::get_param("alpha"));
    int num_iter = std::stoi(husky::Context::get_param("n_iter"));
    auto format = husky::Context::get_param("format");
    husky::lib::ml::DataFormat data_format;
    if (format == "libsvm") {
        data_format = husky::lib::ml::kLIBSVMFormat;
    } else if (format == "tsv") {
        data_format = husky::lib::ml::kTSVFormat;
    }
    if (husky::Context::get_param("is_sparse") == "true") {
        linear_regression<true>(alpha, num_iter, data_format);
    } else {
        linear_regression<false>(alpha, num_iter, data_format);
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args({
        "hdfs_namenode", "hdfs_namenode_port", "train", "test", "n_iter", "alpha", "format", "is_sparse"
    });
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(initialize);
        return 0;
    }
    return 1;
}
