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

#include <atomic>
#include <cassert>
#include <functional>
#include <memory>
#include <vector>

#include "base/serialization.hpp"
#include "core/utils.hpp"
#include "lib/aggregator_object.hpp"

namespace husky {
namespace lib {

class AggregatorInfo {
   public:
    AggregatorState* value;
    std::atomic_uint num_holder;
    virtual ~AggregatorInfo();
};

template <typename ValueType>
class AggregatorInfoWithInitValue : public AggregatorInfo {
   public:
    ValueType init_value;
};

class AggregatorFactoryBase {
   public:
    static inline void sync() { get_factory().synchronize(); }

    static size_t get_num_aggregator() { return get_factory().num_aggregator_; }

    static size_t get_num_active_aggregator() { return get_factory().num_active_aggregator_; }

    // Note: registration should finish before anyone calls create_factory()
    // Priority can be applied here if there are multiple types of factory
    static void register_factory_constructor(const std::function<AggregatorFactoryBase*()>& ctor) {
        ASSERT_MSG(ctor != nullptr, "Cannot register a nullptr aggregator factory constructor");
        get_factory_constructor() = ctor;
    }

    // Inactivate the factory: all aggregators still accept and store updates but will not synchronize the updates
    static void inactivate() { get_factory().is_active_ = false; }

    // Activate the factory:
    // 1. all aggregators still accept and store udpates
    // 2. only active aggregators will synchronize the updates
    // 3. activating the factory will not turn original inactive aggregators to be active
    static void activate() { get_factory().is_active_ = true; }

    virtual ~AggregatorFactoryBase();

    virtual size_t get_factory_id() = 0;
    virtual size_t get_num_global_factory() = 0;
    virtual size_t get_num_local_factory() = 0;
    virtual size_t get_machine_id() = 0;
    // get the mch id of the factory with id `fid`, should be [0, get_num_machine())
    virtual size_t get_machine_id(size_t fid) = 0;
    virtual size_t get_num_machine() = 0;

   protected:
    static AggregatorFactoryBase& get_factory() {
        if (factory_ == nullptr) {
            factory_ = std::shared_ptr<AggregatorFactoryBase>(create_factory());
            factory_->init_factory();
        }
        return *factory_;
    }

    void sync_aggregator_cache();
    size_t get_local_factory_idx();
    std::vector<AggregatorState*>& get_local_aggs();
    virtual void init_factory();
    virtual void synchronize();
    virtual bool same_machine(size_t fid);
    virtual size_t get_local_center(size_t machine_id, size_t agg_idx);

    // set and get the leader of the factories
    virtual void set_factory_leader(AggregatorFactoryBase*);
    virtual AggregatorFactoryBase* get_factory_leader();

    // send local updates to global centers on other machines
    virtual void send_local_update(std::vector<BinStream>&) = 0;
    // handle the BinStreams received from local centers on other machines
    virtual void on_recv_local_update(const std::function<void(BinStream&)>&) = 0;
    // broadcast aggregators to local centers on other machines
    virtual void broadcast_aggregator(std::vector<BinStream>&) = 0;
    // handle the BinStreams received from global centers on other machines
    virtual void on_recv_broadcast(const std::function<void(BinStream&)>&) = 0;

    // access the local_aggs_ of other local factories when they are ready
    virtual void on_access_other_local_update(const std::function<void(std::vector<AggregatorState*>&)>&) = 0;
    virtual void call_once(const std::function<void()>& lambda) = 0;
    virtual void wait_for_others() = 0;

    // a structure which stores information that is shared by all the factories on the same machine
    class InnerSharedData {
       public:  // public so that AggregatorFactoryBase can access the member freely
        virtual void initialize(AggregatorFactoryBase*);
        virtual ~InnerSharedData();
        std::vector<AggregatorInfo*> global_aggs;
        std::vector<std::vector<size_t>> all_factory;
        std::vector<size_t> global_centers;
        std::vector<size_t> local_centers;
        std::atomic_uint num_holder{0};
    };

    virtual InnerSharedData* create_shared_data() = 0;
    InnerSharedData* get_shared_data();

   private:
    template <typename ValueType>
    friend class Aggregator;

    static std::function<AggregatorFactoryBase*()>& get_factory_constructor() {
        // just to store the factory constructor here
        static std::function<AggregatorFactoryBase*()> factory_constructor = nullptr;
        return factory_constructor;
    }

    static AggregatorFactoryBase* create_factory();

    // Assumption: each factory should create an aggregator together
    // i.e. the amount and the order of aggregators created by each factory must be the same
    // Push the local agg into local_aggs_ and return the global one in global_aggs
    // TODO(zzxx): support user defined Aggregator class, will do when it's in need
    template <typename ValueType>
    AggregatorInfo* create_aggregator(const ValueType& init_val, AggregatorObject<ValueType>* new_local_agg) {
        local_aggs_.push_back(new_local_agg);
        call_once([&] {
            auto* info = new AggregatorInfoWithInitValue<ValueType>();
            // init_val may be a local variable and may be already destroyed when zero lambda is called
            // so here a copy of init value and its destructor are made in advance.
            copy_assign(info->init_value, init_val);
            info->value = new AggregatorObject<ValueType>(
                new_local_agg->get_aggregate_lambda(), [info](ValueType& v) { copy_assign(v, info->init_value); },
                new_local_agg->get_load_lambda(), new_local_agg->get_save_lambda());
            info->value->prepare_value();
            info->num_holder.store(static_cast<uint32_t>(get_num_local_factory()));
            shared_data_->global_aggs.push_back(info);
        });
        ++num_active_aggregator_;
        ++num_aggregator_;
        return shared_data_->global_aggs[local_aggs_.size() - 1];
    }

    void remove_aggregator(AggregatorState* local, AggregatorInfo* global_info);

    AggregatorFactoryBase*& default_factory_leader();

    bool is_active_{true};
    size_t num_active_aggregator_{0};
    size_t num_aggregator_{0};
    size_t local_factory_idx_{0};   // index of this factory among all local factories
    size_t global_factory_idx_{0};  // index of this factory among all global factories
    std::vector<AggregatorState*> local_aggs_;
    InnerSharedData* shared_data_{nullptr};

    static thread_local std::shared_ptr<AggregatorFactoryBase> factory_;
};

// Note for using aggregator:
// 1. An instance of Aggregator created by one thread should not be passed to another thread.
// 2. `Init value` is the initial value of an aggregator, it's the value that an aggregator returns
//   if the aggregator has aggregated nothing.
// 3. `Zero value` is the value set to a local update copy of aggregator ONLY when
//   a. the aggregator is updated using `update_any`;
//   b. the local copy is updated for the first time.
// 4. `Zero value` should satisfies the following property, otherwise the behavior is undefined:
//   `Init value` aggregate `Zero value` == `Init value`
// 5. For the following functions, once they're called, they should be called by all worker thread in the same order:
//   a. to_keep_aggregate()
//   b. to_reset_each_iter()
//   c. activate()
//   d. inactivate()
//   e. constructor
//   f. destructor
//  Otherwise, the behavior is undefined and exceptions may be thrown.
// 6. Inactive aggregators still accept and store updates but will not synchronize the updates
//
// TODO(zzxx): construct AggregateLambdaType `a = sum(a, b)` if sum is given
// TODO(zzxx): only ZeroLambdaType is given and AggregateLambdaType uses the default one
template <typename ValueType>
class Aggregator {
   private:
    typedef std::function<void(ValueType&, const ValueType&)> AggregateLambdaType;
    typedef std::function<void(ValueType&)> ZeroLambdaType;
    typedef std::function<void(BinStream&, ValueType&)> LoadLambdaType;
    typedef std::function<void(BinStream&, const ValueType&)> SaveLambdaType;
    typedef std::function<void(ValueType&)> UpdateAnyLambdaType;

   public:
    Aggregator()
        : Aggregator(ValueType(), [](ValueType& a, const ValueType& b) { a += b; },
                     nullptr,  // [](ValueType& a) { copy_assign(a, ValueType()); },
                     [](BinStream& in, ValueType& b) { in >> b; },
                     [](BinStream& out, const ValueType& b) { out << b; }) {}

    explicit Aggregator(const ValueType& init_val)
        : Aggregator(init_val, [](ValueType& a, const ValueType& b) { a += b; },
                     nullptr,  // [](ValueType& a) { copy_assign(a, ValueType()); },
                     [](BinStream& in, ValueType& b) { in >> b; },
                     [](BinStream& out, const ValueType& b) { out << b; }) {}

    Aggregator(const ValueType& init_val, const AggregateLambdaType& aggregate, const ZeroLambdaType& zero = nullptr)
        : Aggregator(init_val, aggregate, zero, [](BinStream& in, ValueType& b) { in >> b; },
                     [](BinStream& out, const ValueType& b) { out << b; }) {}

    Aggregator(const ValueType& init_val, const AggregateLambdaType& aggregate, const ZeroLambdaType& zero,
               const LoadLambdaType& load, const SaveLambdaType& save) {
        shared_cnt = new int(1);
        // create aggregator first
        local_agg_ = new AggregatorObject<ValueType>(aggregate, zero, load, save);
        // then get the pointer of aggregator
        global_agg_info_ = AggregatorFactoryBase::get_factory().create_aggregator(init_val, local_agg_);
        global_agg_ = reinterpret_cast<AggregatorObject<ValueType>*>(global_agg_info_->value);
    }

    Aggregator(const Aggregator<ValueType>& b)
        : local_agg_(b.local_agg_),
          global_agg_(b.global_agg_),
          global_agg_info_(b.global_agg_info_),
          shared_cnt(b.shared_cnt) {
        ++*shared_cnt;
    }

    void update(const ValueType& b) { local_agg_->aggregate(b); }

    template <typename UpdateLambdaType, typename U>
    void update(const UpdateLambdaType& lambda, const U& val) {
        lambda(local_agg_->get_value(), val);
    }

    void update_any(const UpdateAnyLambdaType& lambda) { lambda(local_agg_->get_value()); }

    ValueType& get_value() { return global_agg_->get_value(); }

    void to_keep_aggregate() { local_agg_->set_keep_aggregate(); }

    void to_reset_each_iter() { local_agg_->set_reset_each_iter(); }

    void activate() {
        AggregatorFactoryBase::get_factory().num_active_aggregator_ += !(local_agg_->is_active());
        local_agg_->set_active();
    }

    void inactivate() {
        AggregatorFactoryBase::get_factory().num_active_aggregator_ -= local_agg_->is_active();
        local_agg_->set_inactive();
    }

    ~Aggregator() {
        if (--*shared_cnt == 0) {
            AggregatorFactoryBase::get_factory().remove_aggregator(local_agg_, global_agg_info_);
            delete shared_cnt;
        }
    }

   private:
    AggregatorInfo* global_agg_info_ = nullptr;
    AggregatorObject<ValueType>* global_agg_ = nullptr;
    AggregatorObject<ValueType>* local_agg_ = nullptr;
    int* shared_cnt = nullptr;
};

}  // namespace lib
}  // namespace husky
