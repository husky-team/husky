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

#include "lib/aggregator.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#ifdef BBQ
#include "base/blocked_queue.hpp"
#endif
#include "base/thread_support.hpp"
#include "core/context.hpp"
#include "core/shuffler.hpp"
#include "core/worker_info.hpp"

namespace husky {
namespace lib {

const bool AGG_ON = true;
const bool AGG_OFF = false;

std::mutex AggregatorWorker::lock;
void* AggregatorWorker::tmp_buf = nullptr;
thread_local std::shared_ptr<AggregatorWorker> AggregatorWorker::agg_worker;

int AggregatorWorker::get_num_global_workers() { return Context::get_worker_info()->get_num_workers(); }

int AggregatorWorker::get_num_local_workers() { return Context::get_worker_info()->get_num_local_workers(); }

int AggregatorWorker::get_global_wid() { return Context::get_global_tid(); }

int AggregatorWorker::get_machine_id() { return Context::get_worker_info()->get_proc_id(); }

int AggregatorWorker::get_machine_id(int global_wid) { return Context::get_worker_info()->get_proc_id(global_wid); }

int AggregatorWorker::global_assign() {
    auto& vec = *worker_info;
    while (true) {
        if (assign_x == vec.size()) {
            assign_x = 0;
            assign_y++;
            if (assign_y == max_num_row)
                assign_y = 0;
        }
        if (vec[assign_x].size() > assign_y)
            break;
        assign_x++;
    }
    return vec[assign_x++][assign_y];
}

int AggregatorWorker::local_assign() {
    if (assign_i == (*worker_info)[local_vec].size()) {
        assign_i = 0;
    }
    return (*worker_info)[local_vec][assign_i++];
}

bool AggregatorWorker::same_machine(int worker_id) { return this->get_machine_id(worker_id) == this->get_machine_id(); }

void AggregatorWorker::post_execute() {
    std::vector<BinStream> bin_agg;
    bin_agg.resize(this->get_num_global_workers());

    if (*agg_option == AGG_OFF || machine_aggs->empty())
        return;

    local_aggs->commit();
#ifdef BBQ
    static BlockedQueue queue(this->get_num_local_workers());
    queue.push_back(this->block_queue_gen++, this_col);
#endif

    // 1. worker drags updates from peers and send public updates to corresponding masters
    for (int i = 0, sz = this->get_num_local_workers(); i < sz; i++) {
#ifdef BBQ
        int next = queue.pop(i);
#else
        int next = i;
#endif
        auto& local_update = worker_aggs[next].access();
        for (auto& task : local_task) {
            auto& agg = local_update[task];
            if (agg->is_non_zero()) {
                (*machine_aggs)[task].update->aggregate(agg);
                agg->set_zero();
            }
        }
        worker_aggs[next].leave();
    }

    // Reset the aggregate value for read. Notice that this is done
    // after the barrier above, so that the value for read won't be
    // reset while others are still reading it
    for (auto& i : local_task) {
        AggregatorObject* value = (*machine_aggs)[i].value;
        value->set_no_update();
        value->set_zero();
    }

    for (auto& task : local_task) {
        auto& tuple = (*machine_aggs)[task];
        if (tuple.update->is_non_zero()) {
            if (same_machine(tuple.master_id)) {
                tuple.value->aggregate(tuple.update);
                tuple.value->set_non_zero();
                tuple.value->set_need_update();
            } else {
                auto& destination = bin_agg[tuple.master_id];
                destination << task;
                tuple.update->save(destination);
            }
            tuple.update->set_zero();
        }
    }

    _channel.send(bin_agg);

    // 3. each master receives its task and calculate the aggregation value
    {
        while (_channel.poll()) {
            auto bin_push = _channel.recv();
            for (Name key; bin_push.size();) {
                bin_push >> key;
#ifndef AGG_SAFE_MODE
                auto& tuple = (*machine_aggs)[key];
#else
                auto iter = machine_aggs->find(key);
                ASSERT_MSG(iter != machine_aggs->end(), "System Error: Invalied key deserialized from messages!");
                auto& tuple = iter->second;
#endif

                tuple.update->set_non_zero();
                tuple.update->load(bin_push);
                tuple.update->set_zero();

                tuple.value->aggregate(tuple.update);
                tuple.value->set_non_zero();
                tuple.value->set_need_update();
            }
        }
    }
    // 4. send the aggregation value to all the leaders
    wait_for_others();  // avoid workers from aggregating aggs while aggs are under serialization
    for (auto& i : global_task) {
        auto& tuple = (*machine_aggs)[i];
        if (tuple.value->need_update()) {
            for (int r = worker_info->size() - 1; r >= 0; r--) {
                auto& vec = (*worker_info)[r];
                int dst = vec[this_col % vec.size()];
                if (dst == this->get_machine_id())  // same machine
                    continue;
                auto& destination = bin_agg[dst];
                destination << i;
                tuple.value->save(destination);
            }
            tuple.value->set_no_update();
        }
    }

    _channel.send(bin_agg);

    // 5. local leader update public values
    {
        while (_channel.poll()) {
            auto bin_push = _channel.recv();
            for (Name key; bin_push.size();) {
                bin_push >> key;
#ifndef AGG_SAFE_MODE
                auto& tuple = (*machine_aggs)[key];
#else
                auto iter = machine_aggs->find(key);
                ASSERT_MSG(iter != machine_aggs->end(), "System Error: Invalied key deserialized from messages!");
                auto& tuple = iter->second;
#endif
                tuple.value->set_non_zero();
                tuple.value->load(bin_push);
            }
        }
    }
    // if we don't wait here, someone will use an agg before it's updated
    // we should not wait here.
    wait_for_others();
}

void AggregatorWorker::_set_agg_option(AggregatorObject& object, char option) {
    switch (option) {
    case AGG_RESET_EACH_ROUND:
        object.to_reset_each_round();
        break;
    case AGG_KEEP_AGGREGATE:
        object.to_keep_aggregate();
        break;
    default:
        ASSERT_MSG(false, "Illegal option for aggregation");
    }
}

void AggregatorWorker::_set_agg_option(const Name& agg_key, char option) {
    ASSERT_MSG(machine_aggs->find(agg_key) != machine_aggs->end(), "Aggregator NOT exists: set_agg_option");
    _set_agg_option(*(*machine_aggs)[agg_key].value, option);
}

void AggregatorWorker::_remove_agg(const Name& agg_key) {
    auto& tuple = (*machine_aggs)[agg_key];
    if (tuple.cache == is_worker_cached) {
        delete local_aggs->storage()[agg_key];
        local_aggs->storage().erase(agg_key);
    }
    if (tuple.master_id == this->get_global_wid()) {
        ASSERT_MSG(machine_aggs->find(agg_key) != machine_aggs->end(), "Aggregator NOT exists, remove_agg");
#ifdef AGG_SAFE_MODE
        log_msg("Remove aggregator " + std::to_string(agg_key.val));
#endif
        for (int i = global_task.size() - 1; i >= 0; i--) {
            if (global_task[i] == agg_key) {
                std::swap(global_task[i], global_task.back());
                global_task.resize(global_task.size() - 1);
                break;
            }
        }
    }
    if (tuple.local_master_id == this->get_global_wid()) {
        for (int i = local_task.size() - 1; i >= 0; i--) {
            if (local_task[i] == agg_key) {
                std::swap(local_task[i], local_task.back());
                local_task.resize(local_task.size() - 1);
                break;
            }
        }
    }
    // prevent some one from using this aggregator while the leader is about to delete it
    wait_for_others();
    if (is_local_leader()) {
        auto& tuple = (*machine_aggs)[agg_key];
        delete tuple.value;
        delete tuple.update;
        machine_aggs->erase(agg_key);
    }
}

void AggregatorWorker::wait_for_others() { barrier->wait(); }

int AggregatorWorker::get_next_agg_id() {
    static thread_local int next_id = 0;
    return ++next_id;
}

void AggregatorWorker::AGG_enable(bool option) { *agg_option = option; }

bool AggregatorWorker::is_local_leader() { return this == leader; }

AggregatorWorker::AggregatorWorker() : barrier(nullptr) {
    _channel.default_setup(std::bind(&AggregatorWorker::post_execute, this));
    lock.lock();
    int num_local_workers = this->get_num_local_workers();
    if (tmp_buf == nullptr) {
        barrier = new Barrier(num_local_workers);
        tmp_buf = reinterpret_cast<void*>(barrier);
    } else {
        barrier = reinterpret_cast<Barrier*>(tmp_buf);
    }
    lock.unlock();
    wait_for_others();
    int proc_id = this->get_machine_id();
    call_once_each_time([&, this]() {
        tmp_buf = reinterpret_cast<void*>(this);
        machine_aggs = new std::unordered_map<AggKey, AggregatorTuple>();

        std::vector<int> proc_ids(this->get_num_global_workers());
        for (int i = proc_ids.size() - 1; i >= 0; i--)
            proc_ids[i] = this->get_machine_id(i);
        std::sort(proc_ids.begin(), proc_ids.end());
        proc_ids.resize(std::unique(proc_ids.begin(), proc_ids.end()) - proc_ids.begin());
        worker_info = new std::vector<std::vector<int>>(proc_ids.size());
        auto& info = *(worker_info);
        max_num_row = 0;  // somehow I think machines would have different number of worker threads
        for (int i = this->get_num_global_workers() - 1; i >= 0; i--) {
            int idx = lower_bound(proc_ids.begin(), proc_ids.end(), this->get_machine_id(i)) - proc_ids.begin();
            info[idx].push_back(i);
            max_num_row = std::max(max_num_row, info[idx].size());
        }
        local_vec = lower_bound(proc_ids.begin(), proc_ids.end(), proc_id) - proc_ids.begin();

        auto allocator = std::allocator<Accessor<std::unordered_map<AggKey, AggregatorObject*>>>();
        worker_aggs = allocator.allocate(num_local_workers);
        for (int i = 0; i < num_local_workers; i++)
            allocator.construct(worker_aggs + i, "AGG_MAP", num_local_workers);
        agg_option = new bool(AGG_ON);
    });
    leader = reinterpret_cast<AggregatorWorker*>(tmp_buf);
    if (this != leader) {
        machine_aggs = leader->machine_aggs;
        worker_info = leader->worker_info;
        local_vec = leader->local_vec;
        max_num_row = leader->max_num_row;
        agg_option = leader->agg_option;
        worker_aggs = leader->worker_aggs;
    }
    auto& vec = (*worker_info)[local_vec];
    for (this_col = vec.size() - 1; this_col >= 0; this_col--) {
        if (vec[this_col] == this->get_global_wid())
            break;
    }
    (local_aggs = worker_aggs + this_col)->init();
}

AggregatorWorker::~AggregatorWorker() {
    static int left_worker = this->get_num_local_workers();
    for (auto& agg : local_aggs->storage()) {
        delete agg.second;
    }
    lock.lock();
    if (left_worker == 0)
        left_worker = this->get_num_local_workers();
    if (--left_worker == 0) {
        // only the last one will enter here
        for (auto& tuple : *machine_aggs) {
            delete tuple.second.value;
            delete tuple.second.update;
        }
        delete worker_info;
        delete machine_aggs;
        delete barrier;
        delete agg_option;
        auto allocator = std::allocator<Accessor<std::unordered_map<AggKey, AggregatorObject*>>>();
        for (int i = 0; i < this->get_num_local_workers(); i++)
            allocator.destroy(worker_aggs + i);
        allocator.deallocate(worker_aggs, this->get_num_local_workers());
        tmp_buf = nullptr;
        barrier = nullptr;
        machine_aggs = nullptr;
        worker_info = nullptr;
        agg_option = nullptr;
    }
    lock.unlock();
}

int AggregatorWorker::get_num_aggregator() { return machine_aggs->size(); }

bool AggregatorWorker::is_agg_existed(const std::string& agg_name) {
    return machine_aggs->find(Name("list_" + agg_name)) != machine_aggs->end();
}

void AggregatorWorker::set_agg_option(const std::string& agg_name, char option) {
    _set_agg_option(Name("list_" + agg_name), option);
}

void AggregatorWorker::remove_agg(const std::string& agg_name) { _remove_agg(Name("list_" + agg_name)); }

AggregatorWorker& AggregatorWorker::get() {
    if (agg_worker.get() == nullptr) {
        agg_worker = std::make_shared<AggregatorWorker>();
    }
    return *agg_worker;
}

h3::AggregatorChannel& AggregatorWorker::get_channel() { return this->_channel; }

}  // namespace lib
}  // namespace husky
