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
#include <mutex>
#include <string>
#include <vector>

#include "base/serialization.hpp"
#include "base/thread_support.hpp"
#include "core/channel/aggregator_channel.hpp"
#include "core/context.hpp"
#include "core/shuffler.hpp"

namespace husky {
namespace lib {

using Base::BinStream;

// HBarrier from core/misc.
typedef HBarrier Barrier;
// typedef boost::barrier Barrier;

template <typename T>
void new_copy(T& a, const T& b) {
    a = b;
}

enum class CacheType { is_worker_cached, non_worker_cached };

struct AggregatorInfo {
    AggregatorObject* value;
    AggregatorObject* update;
    std::mutex lock;
    int master_id;
    int local_master_id;
    CacheType cache;
};

class AggregatorWorker {
   public:
    virtual ~AggregatorWorker();

    int get_num_global_workers();
    int get_num_local_workers();
    int get_global_wid();
    int get_machine_id();
    int get_machine_id(int global_wid);

    void wait_for_others();
    int get_next_agg_id();
    void AGG_enable(bool option);
    bool is_local_leader();
    AggregatorWorker();
    int get_num_aggregator();

    AggregatorChannel& get_channel();
    static AggregatorWorker& get();
    static h3::AggregatorChannel& channel() { return AggregatorWorker::get().get_channel(); }

    template <typename AggregatorT, typename ValueT, typename... Args>
    void create_agg(const std::string& agg_name, const ValueT& init_val, CacheType cache, Args&&... args) {
        _create_agg<AggregatorT>(Name("list_" + agg_name), init_val, cache, args...);
    }

    template <typename AggregatorT, typename ValueT, typename... Args>
    void create_agg(const std::string& agg_name, const ValueT& init_val, Args&&... args) {
        _create_agg<AggregatorT>(Name("list_" + agg_name), init_val, is_worker_cached, args...);
    }

    template <typename AggregatorT, typename ValueT, typename... Args>
    void create_agg(const std::string& agg_name, CacheType cache, Args&&... args) {
        _create_agg<AggregatorT>(Name("list_" + agg_name), ValueT(), cache, args...);
    }

    template <typename AggregatorT, typename ValueT, typename... Args>
    void create_agg(const std::string& agg_name, Args&&... args) {
        _create_agg<AggregatorT>(Name("list_" + agg_name), ValueT(), is_worker_cached, args...);
    }

    bool is_agg_existed(const std::string& agg_name);

    template <typename AggregatorT, typename ValueT>
    void update_agg(const std::string& agg_name, const ValueT& value) {
        _update_agg<AggregatorT>(Name("list_" + agg_name), value);
    }

    template <typename AggregatorT>
    typename AggregatorT::ValueT& get_agg_value(const std::string& agg_name) {
        return _get_agg_value<AggregatorT>(Name("list_" + agg_name));
    }

    void set_agg_option(const std::string& agg_name, char option);

    void remove_agg(const std::string& agg_name);

   protected:
    void post_execute();
    void push_to_obj_list(BinStream&);
    bool same_machine(int worker_id);

    /* * Some Checks
     * 1. Subclass check
     * 2. duplicate key check
     * 3. key existence check
     * */
    template <typename AggregatorT, typename ValueT, typename... Args>
    AggregatorTuple& _create_agg(const AggKey& agg_key, const ValueT& init_val, CacheType cache, Args&&... args) {
        static_assert(std::is_base_of<AggregatorObject, AggregatorT>::value,
                      "Cannot create an aggregator which is NOT a subclass of BaseAggregator!");
        if (cache == is_worker_cached) {
            (local_aggs->storage()[agg_key] = new AggregatorT(args...))->to_reset_each_round();
        }
        // other workers may be using the machine_aggs ...
        wait_for_others();
        if (is_local_leader()) {
            ASSERT_MSG(machine_aggs->find(agg_key) == machine_aggs->end(),
                       "Could NOT create Aggregators of same names");
            auto& agg = (*machine_aggs)[agg_key];
            agg.value = new AggregatorT(args...);
            static_cast<AggregatorT*>(agg.value)->value = init_val;
            agg.value->set_non_zero();
            agg.update = new AggregatorT(args...);
            agg.update->to_reset_each_round();
            agg.update->set_zero();
            agg.master_id = global_assign();
            agg.cache = cache;
        }
        wait_for_others();
        auto& tuple = (*machine_aggs)[agg_key];
        wait_for_others();
        if (tuple.master_id == this->get_global_wid()) {
            global_task.push_back(agg_key);
            husky::log_msg("New aggregator " + std::to_string(agg_key.val) + " is assigned to worker " +
                           std::to_string(this->get_global_wid()));
        }
        if (local_assign() == this->get_global_wid()) {
            local_task.push_back(agg_key);
            tuple.local_master_id = this->get_global_wid();
        }
        return tuple;
    }

    template <typename AggregatorT, typename ValueT>
    void _update_agg(const Name& agg_key, const ValueT& value) {
        static_assert(std::is_base_of<AggregatorObject, AggregatorT>::value,
                      "Cannot update an aggregator which is NOT a subclass of BaseAggregator!");
        ASSERT_MSG(machine_aggs->find(agg_key) != machine_aggs->end(), "Aggregator NOT exists, update_agg_value");
        auto& tuple = (*machine_aggs)[agg_key];

        if (tuple.cache == is_worker_cached) {
            AggregatorT* agg = static_cast<AggregatorT*>(local_aggs->storage()[agg_key]);
            agg->aggregate(agg->get_value(), value);
        } else {
            AggregatorT* agg = static_cast<AggregatorT*>(tuple.update);
            tuple.lock.lock();
            agg->aggregate(agg->get_value(), value);
            tuple.lock.unlock();
        }
    }

    template <typename AggregatorT>
    void _update_agg(const Name& agg_key, const typename AggregatorT::ValueT& value) {
        static_assert(std::is_base_of<AggregatorObject, AggregatorT>::value,
                      "Cannot update an aggregator which is NOT a subclass of BaseAggregator!");
        ASSERT_MSG(machine_aggs->find(agg_key) != machine_aggs->end(), "Aggregator NOT exists, update_agg");
        auto& tuple = (*machine_aggs)[agg_key];

        if (tuple.cache == is_worker_cached) {
            AggregatorT* agg = static_cast<AggregatorT*>(local_aggs->storage()[agg_key]);
            if (agg->is_non_zero()) {
                agg->aggregate(agg->get_value(), value);
            } else {
                agg->value = value;
                agg->set_non_zero();
            }
        } else {
            AggregatorT* agg = static_cast<AggregatorT*>(tuple.update);
            tuple.lock.lock();
            if (agg->is_non_zero()) {
                agg->aggregate(agg->get_value(), value);
            } else {
                agg->value = value;
                agg->set_non_zero();
            }
            tuple.lock.unlock();
        }
    }

    void _set_agg_option(const Name& agg_key, char option);

    void _set_agg_option(AggregatorObject& agg_key, char option);

    void _remove_agg(const Name& agg_key);

    template <typename AggregatorT>
    typename AggregatorT::ValueT& _get_agg_value(const Name& agg_key) {
        static_assert(std::is_base_of<AggregatorObject, AggregatorT>::value,
                      "Cannot get value from an aggregator which is NOT a subclass of BaseAggregator!");
        ASSERT_MSG(machine_aggs->find(agg_key) != machine_aggs->end(), "Aggregator NOT exists, get_agg_value");
        return (reinterpret_cast<AggregatorT*>((*machine_aggs)[agg_key].value))->get_value();
    }

   private:
    static thread_local std::shared_ptr<AggregatorWorker> agg_worker;

    typedef Name AggKey;

    static std::mutex lock;
    static void* tmp_buf;
    bool* agg_option;
    AggregatorWorker* leader;
    Barrier* barrier;
    std::unordered_map<AggKey, AggregatorTuple>* machine_aggs;
    Accessor<std::unordered_map<AggKey, AggregatorObject*>>* worker_aggs;
    Accessor<std::unordered_map<AggKey, AggregatorObject*>>* local_aggs;

#ifdef BBQ
    int block_queue_gen = 0;
#endif
    int assign_x = 0, assign_y = 0, assign_i = 0, local_vec, this_col;
    size_t max_num_row;
    std::vector<std::vector<int>>* worker_info;
    std::vector<AggKey> local_task, global_task;

    int global_assign();

    int local_assign();

    friend class AggregatorChannel;
    template <typename ValueType>
    friend class Aggregator;

    h3::AggregatorChannel _channel;
};

template <typename ValueT>
class Aggregator {
    typedef Name AggKey;
    typedef std::function<void(ValueT&, const ValueT&)> AggregateType;
    typedef std::function<void(ValueT&)> ZeroType;

    class AggregatorType : public BaseAggregator<ValueT> {
       public:
        AggregatorType() : _aggregate(nullptr), _zero(nullptr) {}
        AggregateType _aggregate;
        ZeroType _zero;
        void zero(ValueT& v) { _zero(v); }
        void aggregate(ValueT& a, const ValueT& b) { _aggregate(a, b); }
    };

    const Name self_key;
    int* reference_count;
    AggregatorTuple* tuple;  // I am very satisfied that std::unordered_map is pointer-safe
    AggregatorType* object;

   protected:
    Aggregator(const AggKey& name, const ValueT& init_val, AggregateType lambda, ZeroType zero, CacheType worker_cached,
               int* ref_count)
        : self_key(name), reference_count(ref_count) {
        auto& worker = AggregatorWorker::get();
        tuple = &(worker._create_agg<AggregatorType>(self_key, init_val, worker_cached));
        object = static_cast<AggregatorType*>(worker.local_aggs->storage()[self_key]);
        object->_aggregate = lambda;
        if (zero == nullptr)
            zero = [](ValueT& v) { v = ValueT(); };
        object->_zero = zero;
        if (worker.is_local_leader()) {
            auto value = static_cast<AggregatorType*>(tuple->value);
            auto update = static_cast<AggregatorType*>(tuple->update);
            update->_aggregate = value->_aggregate = lambda;
            update->_zero = value->_zero = zero;
        }
    }
    void finalize() {
        // worker may be no longer exists and neither the aggregators
        // if (!SessionLocalBase::is_session_end()) {
        AggregatorWorker::get()._remove_agg(self_key);
        tuple = nullptr;
        object = nullptr;
        // }
        delete reference_count;
        reference_count = nullptr;
    }

   public:
    Aggregator(const Aggregator<ValueT>& b)
        : self_key(b.self_key), reference_count(b.reference_count), tuple(b.tuple), object(b.object) {
        if (reference_count)
            (*reference_count)++;
    }
    explicit Aggregator(AggregateType lambda, ZeroType zero = nullptr, CacheType worker_cached = is_worker_cached)
        : Aggregator(Name("tmp_" + std::to_string(AggregatorWorker::get().get_next_agg_id())), ValueT(), lambda, zero,
                     worker_cached, new int(1)) {}
    Aggregator(const ValueT& init_val, AggregateType lambda, ZeroType zero = nullptr,
               CacheType worker_cached = is_worker_cached)
        : Aggregator(Name("tmp_" + std::to_string(AggregatorWorker::get().get_next_agg_id())), init_val, lambda, zero,
                     worker_cached, new int(1)) {}
    Aggregator(const std::string& name, const ValueT& init_val, AggregateType lambda, ZeroType zero = nullptr,
               CacheType worker_cached = is_worker_cached)
        : Aggregator(Name("list_" + name), init_val, lambda, zero, worker_cached, nullptr) {}
    // this is used for getting a aggregator from existing ones.
    explicit Aggregator(const std::string& name) : self_key(Name("list_" + name)), reference_count(nullptr) {
        auto& worker = AggregatorWorker::get();
        tuple = &(*(worker.machine_aggs))[self_key];
        if (tuple->cache == is_worker_cached) {
            object = static_cast<AggregatorType*>(worker.local_aggs->storage()[self_key]);
        }
    }
    Aggregator& operator=(const Aggregator<ValueT>& b) {
        if (tuple != b.tuple) {
            if (reference_count && (--(*reference_count)) == 0) {
                finalize();
            }
            self_key = b.self_key;
            tuple = b.tuple;
            object = b.object;
            if (b.reference_count) {
                reference_count = b.reference_count;
                ++(*reference_count);
            }
        }
        return *this;
    }
    ValueT& get_value() { return static_cast<AggregatorType*>(tuple->value)->get_value(); }
    void aggregate(ValueT& a, const ValueT& b) { object->_aggregate(a, b); }
    template <typename ValueType>
    void update(const ValueType& value) {
        if (tuple->cache == is_worker_cached) {
            // update the local one
            aggregate(object->get_value(), value);
        } else {
            // update the global one directly
            tuple->lock.lock();
            aggregate(static_cast<AggregatorType*>(tuple->update)->get_value(), value);
            tuple->lock.unlock();
        }
    }
    template <typename ValueType, typename AggregateLambda>
    void update(const ValueType& value, const AggregateLambda& aggregate) {
        if (tuple->cache == is_worker_cached) {
            // update the local one
            aggregate(object->get_value(), value);
        } else {
            // update the global one directly
            tuple->lock.lock();
            aggregate(static_cast<AggregatorType*>(tuple->update)->get_value(), value);
            tuple->lock.unlock();
        }
    }
    void update(const ValueT& value) {
        if (tuple->cache == is_worker_cached) {
            if (object->is_non_zero()) {
                aggregate(object->value, value);
            } else {
                object->value = value;
                object->set_non_zero();
            }
        } else {
            tuple->lock.lock();
            AggregatorType* update = static_cast<AggregatorType*>(tuple->update);
            if (update->is_non_zero()) {
                aggregate(update->value, value);
            } else {
                update->value = value;
                update->set_non_zero();
            }
            tuple->lock.unlock();
        }
    }
    void set_agg_option(char option) {
        // simple assignment, no need to lock
        AggregatorWorker::get()._set_agg_option(*tuple->value, option);
    }
    virtual ~Aggregator() {
        if (reference_count && (--(*reference_count) == 0)) {
            finalize();
        }
    }
    static h3::AggregatorChannel& get_channel() { return AggregatorWorker::get().get_channel(); }
};

}  // namespace lib
}  // namespace husky
