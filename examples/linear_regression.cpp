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
// Configuration example:
// train:/datasets/regression/MSD/train
// test:/datasets/regression/MSD/test
// n_iter:10
// alpha:0.1

#include <string>
#include <vector>

#include "core/engine.hpp"
#include "lib/ml/data_loader.hpp"
#include "lib/ml/linear_regression.hpp"
#include "lib/ml/scaler.hpp"
#include "lib/ml/sgd.hpp"

using husky::lib::ml::SparseFeatureLabel;
using husky::lib::ml::ParameterBucket;

void report(std::string msg) { if (husky::Context::get_global_tid() == 0) husky::base::log_msg(msg); }
void linear_regression() {
    auto & train_set = husky::ObjListFactory::create_objlist<SparseFeatureLabel>("train_set");
    auto & test_set = husky::ObjListFactory::create_objlist<SparseFeatureLabel>("test_set");

    // load data
    husky::lib::ml::DataLoader<SparseFeatureLabel> data_loader(husky::lib::ml::kTSVFormat);
    data_loader.load_info(husky::Context::get_param("train"), train_set);
    data_loader.load_info(husky::Context::get_param("test"), test_set);
    int num_features = data_loader.get_num_feature();

    // scale values to [-1, 1]
    // TODO(Tatiana): inconsistent scaling for train and test set may be problematic
    husky::lib::ml::LinearScaler<> scaler_train(num_features);
    husky::lib::ml::LinearScaler<> scaler_test(num_features);
    scaler_train.fit_transform(train_set);
    scaler_test.fit_transform(test_set);

    double alpha = std::stod(husky::Context::get_param("alpha"));
    int num_iter = std::stoi(husky::Context::get_param("n_iter"));

    // initialize linear regression model
    husky::lib::ml::LinearRegression<SparseFeatureLabel, ParameterBucket<double>> lr(num_features);
    lr.report_per_round = true;  // report training error per round
    lr.train<husky::lib::ml::SGD<SparseFeatureLabel, ParameterBucket<double>>>(train_set, num_iter, alpha);

    report("The error on training set = " + std::to_string(lr.avg_error(train_set)));
    report("The score on training set = " + std::to_string(lr.score(train_set)));
    // validation
    report("The error on testing set = " + std::to_string(lr.avg_error(test_set)));
    report("The score on testing set = " + std::to_string(lr.score(test_set)));
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("train");
    args.push_back("test");
    args.push_back("n_iter");
    args.push_back("alpha");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(linear_regression);
        return 0;
    }
    return 1;
}
