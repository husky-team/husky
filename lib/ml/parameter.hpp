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
#include <string>
#include <vector>

#include "core/utils.hpp"
#include "lib/aggregator.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/vector.hpp"

namespace husky {
namespace lib {
namespace ml {

template <typename T = double>  // type of the parameters
class ParameterBase {
   public:
    // constructors
    ParameterBase() = default;
    explicit ParameterBase(int _num_param) : num_param_(_num_param) {}

    // get all parameters
    virtual Vector<T, false> get_all_param() = 0;

    // get one parameter by index
    virtual T param_at(int idx) = 0;

    // update parameter
    virtual void update(int idx, T val) = 0;

    // regularize parameter
    // TODO(Tatiana): implement as needed
    virtual void regularize(double regulator) {}

    // present parameter
    void present() {
        Vector<T, false> param = get_all_param();
        int idx = 0;
        for (T val : param) {
            base::log_msg("Parameter " + std::to_string(++idx) + ": " + std::to_string(val));
        }
    }

    int get_num_param() { return num_param_; }

   protected:
    int num_param_ = -1;
};

template <typename T = double>
class ParameterBucket : public ParameterBase<T> {
    using VecT = Vector<T, false>;

   public:
    // constructors
    ParameterBucket() : ParameterBase<T>() {}
    explicit ParameterBucket(int _num_param) : ParameterBucket() { init(_num_param); }

    Vector<T, false> get_all_param() override {
        assert(!this->vector_agg.empty());
        VecT param_vec(this->num_param_);
        int count = 0;
        for (auto& bucket_agg : this->vector_agg) {
            const VecT& bucket = bucket_agg.get_value();
            for (int i = 0; i < bucket.get_feature_num() && count < this->num_param_; i++, count++) {
                param_vec[count] = bucket[i];
            }
        }
        return param_vec;
    }

    T param_at(int idx) override {
        ASSERT_MSG(this->block_size > 0, "Parameter block size is 0.");
        ASSERT_MSG(!this->vector_agg.empty(), "Parameter is not initialized.");
        double result = this->vector_agg[idx / this->block_size].get_value()[idx % this->block_size];
        return this->vector_agg[idx / this->block_size].get_value()[idx % this->block_size];
    }

    // the default update function
    void update(int idx, T val) override {
        ASSERT_MSG(this->block_size > 0, "Parameter block size is 0.");
        ASSERT_MSG(!this->vector_agg.empty(), "Parameter is not initialized.");
        this->vector_agg[idx / this->block_size].update_any([&](VecT& a) { a[idx % this->block_size] += val; });
    }

    // initialize aggregators
    // TODO(Tatiana): implement random number initialization?
    void init(int _num_param, double initial_val = 0.0) {
        this->num_param_ = _num_param;
        // split aggregation of parameters
        const int num_worker = Context::get_worker_info()->get_num_workers();
        this->block_size = (_num_param > num_worker) ? std::ceil((0.0 + _num_param) / num_worker) : 1;

        vector_agg.reserve(num_worker);

        for (int i = 0; i < num_worker; i++) {
            if (i * block_size < _num_param) {
                vector_agg.push_back(lib::Aggregator<VecT>(VecT(block_size, initial_val),
                                                           [](VecT& a, const VecT& b) { a += b; },
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
