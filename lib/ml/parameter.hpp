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
#include <string>
#include <vector>

#include "core/engine.hpp"
#include "lib/aggregator_factory.hpp"

namespace husky {
namespace lib {
namespace ml {

template <typename T = double>  // type of the parameters
class ParameterBase {
   public:
    // constructors
    ParameterBase() = default;
    ParameterBase(std::function<std::vector<T>()> _get_all_param_func, std::function<T(int)> _param_at_func,
                  std::function<void(int, T)> _update_func, int _num_param)
        : get_all_param_func(_get_all_param_func),
          param_at_func(_param_at_func),
          update_func(_update_func),
          num_param_(_num_param) {}

    // get all parameters
    std::vector<T> get_all_param() {
        assert(this->get_all_param_func != nullptr);
        return get_all_param_func();
    }

    // get one parameter by index
    T param_at(int idx) {
        assert(param_at_func != nullptr);
        return param_at_func(idx);
    }

    // update parameter
    void update(int idx, T val) {
        assert(update_func != nullptr);
        update_func(idx, val);
    }

    // regularize parameter
    void regularize(double regulator) {
        ASSERT_MSG(regularize_func_ != nullptr, "regularize function not specified");
        regularize_func_(regulator);
    }

    // set get_all_param_func
    void set_get_all_param(std::function<std::vector<T>()> _get_all_param_func) {
        this->get_all_param_func = _get_all_param_func;
    }

    // set param_at_func
    void set_param_at(std::function<T(int)> _param_at_func) { this->param_at_func = _param_at_func; }

    // set update_func
    void set_update(std::function<void(int, T)> _update_func) { this->update_func = _update_func; }

    // set regularize function
    void set_regularize(std::function<void(double)> _regularize_func) { this->regularize_func_ = _regularize_func; }

    // present parameter
    void present() {
        assert(this->get_all_param_func != nullptr);
        std::vector<T> param = get_all_param_func();
        int idx = 0;
        for (T val : param) {
            base::log_msg("Parameter " + std::to_string(++idx) + ": " + std::to_string(val));
        }
    }

    int get_num_param() { return num_param_; }

   protected:
    std::function<std::vector<T>()> get_all_param_func = nullptr;
    std::function<T(int)> param_at_func = nullptr;
    std::function<void(int, T)> update_func = nullptr;
    std::function<void(double)> regularize_func_ = nullptr;
    int num_param_ = -1;
};

template <typename T = double>
class ParameterBucket : public ParameterBase<T> {
    using VecT = std::vector<T>;

   public:
    // constructors
    ParameterBucket() : ParameterBase<T>() {
        auto& ac = husky::lib::AggregatorFactory::get_channel();
        this->get_all_param_func = [this]() {
            assert(!this->vector_agg.empty());
            VecT param_vec;
            int count = 0;
            for (auto& bucket_agg : this->vector_agg) {
                const VecT& bucket = bucket_agg.get_value();
                for (int i = 0; i < bucket.size() && count < this->num_param_; i++, count++) {
                    param_vec.push_back(bucket[i]);
                }
            }
            return param_vec;
        };
        this->param_at_func = [this](int idx) {
            ASSERT_MSG(this->block_size > 0, "Parameter block size is 0.");
            ASSERT_MSG(!this->vector_agg.empty(), "Parameter is not initialized.");
            double result = this->vector_agg[idx / this->block_size].get_value()[idx % this->block_size];
            return this->vector_agg[idx / this->block_size].get_value()[idx % this->block_size];
        };

        // the default update function
        this->update_func = [this](int idx, T val) {
            ASSERT_MSG(this->block_size > 0, "Parameter block size is 0.");
            ASSERT_MSG(!this->vector_agg.empty(), "Parameter is not initialized.");
            this->vector_agg[idx / this->block_size].update_any([&](VecT& a) { a[idx % this->block_size] += val; });
        };
    }
    explicit ParameterBucket(int _num_param) : ParameterBucket() { init(_num_param); }

    // initialize aggregators
    void init(int _num_param, double initial_val = 0.0) {
        this->num_param_ = _num_param;
        // split aggregation of parameters
        const int num_worker = Context::get_worker_info()->get_num_workers();
        this->block_size = (_num_param > num_worker) ? std::ceil((0.0 + _num_param) / num_worker) : 1;

        vector_agg.reserve(num_worker);

        for (int i = 0; i < num_worker; i++) {
            if (i * block_size < _num_param) {
                vector_agg.push_back(lib::Aggregator<VecT>(VecT(block_size, initial_val),
                                                           [](VecT& a, const VecT& b) {
                                                               for (int j = 0; j < a.size(); j++)
                                                                   a[j] += b[j];
                                                           },
                                                           [this](VecT& a) { a = VecT(this->block_size, (T) 0.0); }));
            }
        }
    }

    // sync
    void sync() { husky::lib::AggregatorFactory::sync(); }

   protected:
    std::vector<husky::lib::Aggregator<VecT>> vector_agg;
    int block_size = 0;
};

}  // namespace ml
}  // namespace lib
}  // namespace husky
