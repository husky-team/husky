// Copyright 2017 Husky Team
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

#include <algorithm>
#include <cmath>
#include <functional>
#include <random>
#include <string>
#include <vector>

#include "boost/tokenizer.hpp"

#include "Eigen/Dense"
#include "Eigen/Sparse"

#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/vector.hpp"

namespace husky {

class Image {
   public:
    using KeyT = int;

    Image() = default;
    Image(const Image& other) {
        key = other.key;
        feature = other.feature;
        label = other.label;
    }
    Image(KeyT k, const lib::SparseVectorXd& f, int l) {
        key = k;
        feature = f;
        label = l;
    }

    const KeyT& id() const { return key; }

    KeyT key;
    lib::SparseVectorXd feature;
    int label;

    friend BinStream& operator<<(BinStream& stream, const Image& image) {
        return stream << image.key << image.feature << image.label;
    }

    friend BinStream& operator>>(BinStream& stream, Image& image) {
        return stream >> image.key >> image.feature >> image.label;
    }
};

typedef Eigen::VectorXd VectorT;
typedef Eigen::MatrixXd MatrixT;

namespace base {

BinStream& operator<<(BinStream& stream, const MatrixT& matrix) {
    stream << size_t(matrix.rows());
    stream << size_t(matrix.cols());
    for (int i = 0; i < matrix.rows(); i++)
        for (int j = 0; j < matrix.cols(); j++)
            stream << i << j << (double) matrix(i, j);
}

BinStream& operator>>(BinStream& stream, MatrixT& matrix) {
    size_t row_length, col_length;
    stream >> row_length >> col_length;
    matrix = MatrixT(row_length, col_length);
    double value;
    for (int i = 0; i < row_length; i++) {
        for (int j = 0; j < col_length; j++) {
            stream >> i >> j >> value;
            matrix.coeffRef(i, j) = value;
        }
    }
    return stream;
}

}  // namespace base

class Network {
   public:
    explicit Network(std::vector<int>&& sizes) {
        num_layers_ = sizes.size();
        layer_sizes_ = std::move(sizes);
        biases_.resize(num_layers_);
        weights_.resize(num_layers_);
        delta_biases_.resize(num_layers_);
        delta_weights_.resize(num_layers_);

        auto& sync_list = ObjListStore::create_objlist<Image>();
        sync_list.add_object(Image());
        auto& bc = ChannelStore::create_broadcast_channel<int, VectorT>(sync_list);
        auto& wc = ChannelStore::create_broadcast_channel<int, MatrixT>(sync_list);
        list_execute(sync_list, [&](Image&) {
            if (Context::get_global_tid() != 0)
                return;
            auto normal_random = [=](double) {
                thread_local boost::mt19937 gen;
                thread_local boost::normal_distribution<> random(0.0, 1.0);
                return random(gen);
            };
            for (int i = 1; i < num_layers_; i++) {
                int layer_in = layer_sizes_[i - 1];
                int layer_out = layer_sizes_[i];
                biases_[i] = Eigen::VectorXd::Zero(layer_out).unaryExpr(normal_random);
                weights_[i] = Eigen::MatrixXd::Zero(layer_out, layer_in).unaryExpr(normal_random);
                bc.broadcast(i, biases_[i]);
                wc.broadcast(i, weights_[i]);
            }
        });
        list_execute(sync_list, [&](Image&) {
            if (Context::get_global_tid() == 0)
                return;
            for (int i = 1; i < num_layers_; i++) {
                biases_[i] = bc.get(i);
                weights_[i] = wc.get(i);
            }
        });
    }

    void sgd_train(ObjList<Image>& data, ObjList<Image>& test, int epochs, int mini_batch, double learning_rate) {
        std::vector<VectorT> zero_biases(num_layers_);
        std::vector<MatrixT> zero_weights(num_layers_);
        for (int i = 1; i < num_layers_; i++) {
            int layer_in = layer_sizes_[i - 1];
            int layer_out = layer_sizes_[i];
            delta_biases_[i] = Eigen::VectorXd::Zero(layer_out);
            delta_weights_[i] = Eigen::MatrixXd::Zero(layer_out, layer_in);
            zero_biases[i] = Eigen::VectorXd::Zero(layer_out);
            zero_weights[i] = Eigen::MatrixXd::Zero(layer_out, layer_in);
        }

        auto& sync_list = ObjListStore::create_objlist<Image>();
        sync_list.add_object(Image());
        auto& ac = lib::AggregatorFactory::get_channel();

        lib::Aggregator<int> num_samples_agg(0, [](int& a, const int& b) { a += b; });
        list_execute(data, {}, {&ac}, [&](Image& this_obj) { num_samples_agg.update(1); });
        int num_samples = num_samples_agg.get_value();

        lib::Aggregator<std::vector<VectorT>> biases_agg(zero_biases,
            [&](std::vector<VectorT>& a, const std::vector<VectorT>& b) {
                for (int i = 1; i < num_layers_; i++)
                    a[i] += b[i];
            },
            [&](std::vector<VectorT>& v) {
                v.resize(num_layers_);
                for (int i = 1; i < num_layers_; i++) {
                    int layer_out = layer_sizes_[i];
                    v[i] = Eigen::VectorXd::Zero(layer_out);
                }
            });
        lib::Aggregator<std::vector<MatrixT>> weights_agg(zero_weights,
            [&](std::vector<MatrixT>& a, const std::vector<MatrixT>& b) {
                for (int i = 1; i < num_layers_; i++)
                    a[i] += b[i];
            },
            [&](std::vector<MatrixT>& v) {
                v.resize(num_layers_);
                for (int i = 1; i < num_layers_; i++) {
                    int layer_in = layer_sizes_[i - 1];
                    int layer_out = layer_sizes_[i];
                    v[i] = Eigen::MatrixXd::Zero(layer_out, layer_in);
                }
            });
        lib::Aggregator<int> test_samples_agg(0, [](int& a, const int& b) { a += b; });
        lib::Aggregator<int> errors_agg(0, [](int& a, const int& b) { a += b; });
        biases_agg.to_reset_each_iter();
        weights_agg.to_reset_each_iter();
        test_samples_agg.to_reset_each_iter();
        errors_agg.to_reset_each_iter();

        for (int epoch = 0; epoch < epochs; epoch++) {
            data.shuffle();
            int sample = 0;
            list_execute(data, {}, {}, [&](Image& img) {
                backprop(img.feature, img.label);
                if (++sample == mini_batch) {
                    update_mini_batch(sample, learning_rate);
                    sample = 0;
                }
            });
            update_mini_batch(sample, learning_rate);

            // Averaging the model may not be correct.
            for (int i = 1; i < num_layers_; i++) {
                biases_[i] *= (double) data.get_size() / (double) num_samples;
                weights_[i] *= (double) data.get_size() / (double) num_samples;
            }

            list_execute(sync_list, {}, {&ac}, [&](Image&) {
                biases_agg.update(biases_);
                weights_agg.update(weights_);
            });
            biases_ = biases_agg.get_value();
            weights_ = weights_agg.get_value();

            list_execute(test, {}, {&ac}, [&](Image& img) {
                test_samples_agg.update(1);
                if (evaluate(img.feature) != img.label)
                    errors_agg.update(1);
            });
            int test_samples_sum = test_samples_agg.get_value();
            int errors = errors_agg.get_value();
            if (Context::get_global_tid() == 0)
                LOG_I << "Epoch {" << epoch << "} : " << (1.0 - (double) errors / test_samples_sum);
        }
    }

    void update_mini_batch(int sample, double eta) {
        if (sample == 0)
            return;
        for (int i = 1; i < num_layers_; i++) {
            delta_weights_[i] *= (eta / sample);
            weights_[i] -= delta_weights_[i];
            delta_biases_[i] *= (eta / sample);
            biases_[i] -= delta_biases_[i];
            delta_weights_[i].setZero();
            delta_biases_[i].setZero();
        }
    }

    int evaluate(const lib::SparseVectorXd& test_data) {
        std::vector<VectorT> activations;
        std::vector<VectorT> zs;
        feedforward(test_data, activations, zs);
        Eigen::VectorXd::Index max_index;
        activations.back().maxCoeff(&max_index);
        return static_cast<int>(max_index);
    }

   protected:
    void feedforward(const lib::SparseVectorXd& input, std::vector<VectorT>& activations, std::vector<VectorT>& zs) {
        activations.resize(num_layers_);
        zs.resize(num_layers_);
        for (int i = 1; i < num_layers_; i++) {
            if (i == 1)
                zs[i] = sparse_dot(weights_[i], input) + biases_[i];
            else
                zs[i] = weights_[i] * activations[i - 1] + biases_[i];
            activations[i] = sigmod(zs[i]);
        }
    }

    void backprop(const lib::SparseVectorXd& feature, int label) {
        std::vector<VectorT> activations;
        std::vector<VectorT> zs;
        feedforward(feature, activations, zs);

        std::vector<VectorT> delta(num_layers_);
        int last_layer = num_layers_ - 1;
        delta[last_layer] =
            hadamard_product(cost_derivative(activations[last_layer], label), sigmod_prime(zs[last_layer]));

        for (int i = last_layer - 1; i >= 1; i--)
            delta[i] = hadamard_product(weights_[i + 1].transpose() * delta[i + 1], sigmod_prime(zs[i]));
        for (int i = 1; i <= last_layer; i++) {
            if (i == 1) {
                for (lib::SparseVectorXd::InnerIterator it(feature); it; ++it)
                    delta_weights_[i].col(it.index()) += delta[i] * it.value();
            } else {
                delta_weights_[i] += delta[i] * activations[i - 1].transpose();
            }
            delta_biases_[i] += delta[i];
        }
    }

    double sigmod(double z) { return (double) 1.0 / (1.0 + exp((double) -1.0 * z)); }

    VectorT sigmod(const VectorT& z) {
        VectorT result(z.size());
        for (int i = 0; i < z.size(); i++)
            result(i) = sigmod(z(i));
        return result;
    }

    double sigmod_prime(double z) {
        double sigmod_result = sigmod(z);
        return sigmod_result * (1.0 - sigmod_result);
    }

    VectorT sigmod_prime(const VectorT& z) {
        VectorT result(z.size());
        for (int i = 0; i < z.size(); i++)
            result(i) = sigmod_prime(z(i));
        return result;
    }

    VectorT cost_derivative(const VectorT& activation, int y) {
        VectorT result = activation;
        result(y) -= 1.0;
        return result;
    }

    VectorT sparse_dot(const MatrixT& matrix, const lib::SparseVectorXd& vector) {
        VectorT result = Eigen::VectorXd::Zero(matrix.rows());
        for (lib::SparseVectorXd::InnerIterator it(vector); it; ++it)
            result += matrix.col(it.index()) * it.value();
        return result;
    }

    VectorT hadamard_product(const VectorT& a, const VectorT& b) {
        assert(a.size() == b.size());
        VectorT result(a.size());
        for (int i = 0; i < a.size(); i++)
            result(i) = a(i) * b(i);
        return result;
    }

   private:
    int num_layers_;
    std::vector<int> layer_sizes_;
    std::vector<VectorT> biases_;
    std::vector<MatrixT> weights_;
    std::vector<VectorT> delta_biases_;
    std::vector<MatrixT> delta_weights_;
};

thread_local int s_train_count = 0;
thread_local int s_test_count = 0;

void nn() {
    auto& train_infmt = io::InputFormatStore::create_line_inputformat();
    train_infmt.set_input(Context::get_param("train_set"));
    auto& test_infmt = io::InputFormatStore::create_line_inputformat();
    test_infmt.set_input(Context::get_param("test_set"));

    auto& image_train_list = ObjListStore::create_objlist<Image>();
    auto& image_test_list = ObjListStore::create_objlist<Image>();

    auto parse_each_image = [&](boost::string_ref& chunk) {
        boost::char_separator<char> sep(",");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        int index = 0;
        Image img;
        img.feature.resize(28 * 28);
        for (auto& w : tok) {
            if (index == 0) {
                img.label = std::stoi(w);
                index++;
                continue;
            }
            int v = std::stoi(w);
            if (v != 0)
                img.feature.coeffRef(index - 1) = double(v) / double(255);
            index++;
        }
        return img;
    };
    auto parse_train_list = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        Image img = parse_each_image(chunk);
        int tid = Context::get_global_tid();
        img.key = tid * 100000 + s_train_count;
        s_train_count++;
        image_train_list.add_object(img);
    };
    auto parse_test_list = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        Image img = parse_each_image(chunk);
        int tid = Context::get_global_tid();
        img.key = tid * 100000 + s_test_count;
        s_test_count++;
        image_test_list.add_object(img);
    };

    load(train_infmt, parse_train_list);
    globalize(image_train_list);

    load(test_infmt, parse_test_list);
    globalize(image_test_list);

    Network network({784, 30, 10});
    network.sgd_train(image_train_list, image_test_list, 10, 10, 3.0);
}

}  // namespace husky

int main(int argc, char** argv) {
    if (husky::init_with_args(argc, argv)) {
        husky::run_job(husky::nn);
        return 0;
    }
    return 1;
}
