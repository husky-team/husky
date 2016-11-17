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
#include <cassert>
#include <functional>
#include <numeric>
#include <vector>

#include "base/serialization.hpp"
#include "core/utils.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/aggregator_object.hpp"

namespace husky {
namespace lib {

thread_local std::shared_ptr<AggregatorFactoryBase> AggregatorFactoryBase::factory_;

AggregatorInfo::~AggregatorInfo() {
    if (value != nullptr)
        delete value;
}

AggregatorFactoryBase* AggregatorFactoryBase::create_factory() {
    // use registered factory constructor to create a factory
    auto& ctor = get_factory_constructor();
    static std::mutex mutex;
    mutex.lock();
    if (ctor == nullptr) {
        ctor = []() { return new AggregatorFactory(); };
    }
    mutex.unlock();
    return ctor();
}

void AggregatorFactoryBase::init_factory() {
    call_once([&] {
        set_factory_leader(this);
        shared_data_ = create_shared_data();
        shared_data_->initialize(this);
    });
    shared_data_ = get_factory_leader()->shared_data_;
    local_factory_idx_ =
        std::find(shared_data_->local_centers.begin(), shared_data_->local_centers.end(), get_factory_id()) -
        shared_data_->local_centers.begin();
    global_factory_idx_ =
        std::find(shared_data_->global_centers.begin(), shared_data_->global_centers.end(), get_factory_id()) -
        shared_data_->global_centers.begin();
}

AggregatorFactoryBase::~AggregatorFactoryBase() {
    for (auto agg_state : local_aggs_) {
        delete agg_state;
    }
    if (shared_data_->num_holder.fetch_sub(1) == 1) {
        delete shared_data_;
        set_factory_leader(nullptr);
    }
}

/**
 * sync_aggregator_cache is used only in `synchronize`,
 * thus no need to lock shared_data_->global_aggs
 */
void AggregatorFactoryBase::sync_aggregator_cache() {
    // the last one synchronizes the options of its local aggs to global aggs
    if (shared_data_->num_holder.fetch_sub(1) == 1) {
        shared_data_->num_holder.store(get_num_local_factory());
        std::vector<AggregatorInfo*>& global_aggs = shared_data_->global_aggs;
        for (int i = 0; i < local_aggs_.size(); ++i) {
            global_aggs[i]->value->sync_option(*local_aggs_[i]);
        }
        auto first_inactive = std::partition(global_aggs.begin(), global_aggs.end(),
                                             [](AggregatorInfo* info) { return info->value->is_active(); });
        std::partition(first_inactive, global_aggs.end(),
                       [](AggregatorInfo* info) { return !info->value->is_removed(); });
        global_aggs.resize(num_aggregator_);
    }
    auto first_inactive =
        std::partition(local_aggs_.begin(), local_aggs_.end(), [](AggregatorState* agg) { return agg->is_active(); });
    std::partition(first_inactive, local_aggs_.end(), [](AggregatorState* agg) { return !agg->is_removed(); });
    local_aggs_.resize(num_aggregator_);
}

void AggregatorFactoryBase::synchronize() {
    if (!is_active_)
        return;

    sync_aggregator_cache();

    // 1. fetch local updates from other other factories
    size_t num_local_factory = get_num_local_factory();
    size_t num_global_factory = get_num_global_factory();
    std::vector<BinStream> agg_bins(num_global_factory);

    on_access_other_local_update([&](std::vector<AggregatorState*>& updates) {
        if (&local_aggs_ != &updates) {
            for (size_t i = local_factory_idx_; i < num_active_aggregator_; i += num_local_factory) {
                AggregatorState* update = updates[i];
                if (!update->is_zero()) {
                    local_aggs_[i]->aggregate(update);
                    update->set_zero();
                }
            }
        }
    });

    // Reset the aggregate value for read. Notice that this is done
    // after the barrier above, so that the value for read won't be
    // reset while others are still reading it
    for (size_t i = local_factory_idx_; i < num_active_aggregator_; i += num_local_factory) {
        AggregatorState* update = local_aggs_[i];
        AggregatorState* value = shared_data_->global_aggs[i]->value;
        if (value->need_reset_each_iter()) {
            value->set_zero();
        }
        if (!update->is_zero()) {
            size_t global_center = shared_data_->global_centers[i % num_global_factory];
            if (same_machine(global_center)) {
                value->aggregate(update);
                value->set_updated();
                // printf("%lld %s\n", i, value->to_string().c_str());  // debug purpose
            } else {
                BinStream& dest = agg_bins[global_center];
                dest << i;
                update->save(dest);
            }
            update->set_zero();
        }
    }

    // send local updates to global centers
    send_local_update(agg_bins);
    wait_for_others();
    // global centers receive updates from other machines
    on_recv_local_update([&](BinStream& bin) {
        for (size_t idx; bin.size();) {
            // Note that a factory receives updates as a global center here,
            // so be care about the conflict with the for loop above
            bin >> idx;
            ASSERT_MSG(idx < local_aggs_.size(), "Aggregator Fatal Error: Aggregator index out of range.");
            AggregatorState* update = local_aggs_[idx];
            update->load(bin);
            AggregatorState* value = shared_data_->global_aggs[idx]->value;
            // printf("%lld %s %s\n", idx, update->to_string().c_str(), value->to_string().c_str());  // debug purpose
            value->aggregate(update);
            value->set_updated();
        }
        bin.clear();
    });

    // 4. broadcast agg value to local centers on all other machines
    // TODO(zzxx): if global_center and local_center are on the same machine, local <- global
    wait_for_others();  // avoid factory from aggregating aggs while aggs are under serialization
    for (size_t i = global_factory_idx_; i < num_active_aggregator_; i += num_global_factory) {
        AggregatorState* value = shared_data_->global_aggs[i]->value;
        if (value->is_updated()) {
            // printf("%lld %s\n", i, value->to_string().c_str());  // debug purpose
            for (size_t m = 0; m < get_num_machine(); ++m) {
                size_t peer = get_local_center(m, i);  // global factory id
                if (!same_machine(peer)) {
                    auto& bin = agg_bins[peer];
                    bin << i;
                    value->save(bin);
                }
            }
            value->set_non_updated();
        }
    }

    // 5. local centers updates agg value for read
    broadcast_aggregator(agg_bins);
    wait_for_others();
    on_recv_broadcast([&](BinStream& bin) {
        // Note that a factory receives updates as a local center here,
        // so be care about the conflict with the for loop above
        for (size_t idx; bin.size();) {
            bin >> idx;
            ASSERT_MSG(idx < local_aggs_.size(), "Aggregator Fatal Error: Aggregator index out of range.");
            AggregatorState* value = shared_data_->global_aggs[idx]->value;
            value->load(bin);
            value->set_non_zero();
        }
        bin.clear();
    });

    for (size_t i = local_factory_idx_; i < num_active_aggregator_; i += num_local_factory) {
        // global aggs need to care about multi-thread access when the value is zero
        shared_data_->global_aggs[i]->value->prepare_value();
    }

    // In case someone will use an agg before it's updated
    // In case someone will remove an agg while another is deserializing
    // TODO(zzxx): GenerationLock here, the last one increase the generation
    wait_for_others();
}

bool AggregatorFactoryBase::same_machine(size_t fid) { return get_machine_id() == get_machine_id(fid); }

size_t AggregatorFactoryBase::get_local_center(size_t machine_id, size_t agg_idx) {
    auto& all_factory = shared_data_->all_factory[machine_id];
    return all_factory[agg_idx % all_factory.size()];
}

// local and global_info->value will be invalid after calling remove_aggregator
void AggregatorFactoryBase::remove_aggregator(AggregatorState* local, AggregatorInfo* global_info) {
    if (global_info->num_holder.fetch_sub(1) == 1) {
        global_info->value->set_removed();
    }
    num_active_aggregator_ -= local->is_active();
    --num_aggregator_;
    local->set_removed();
}

std::vector<AggregatorState*>& AggregatorFactoryBase::get_local_aggs() { return local_aggs_; }

AggregatorFactoryBase::InnerSharedData* AggregatorFactoryBase::get_shared_data() { return shared_data_; }

size_t AggregatorFactoryBase::get_local_factory_idx() { return local_factory_idx_; }

void AggregatorFactoryBase::set_factory_leader(AggregatorFactoryBase* leader) { default_factory_leader() = leader; }

AggregatorFactoryBase* AggregatorFactoryBase::get_factory_leader() { return default_factory_leader(); }

AggregatorFactoryBase*& AggregatorFactoryBase::default_factory_leader() {
    static AggregatorFactoryBase* leader = nullptr;
    return leader;
}

void AggregatorFactoryBase::InnerSharedData::initialize(AggregatorFactoryBase* factory) {
    size_t num_local_factory = factory->get_num_local_factory();
    size_t num_global_factory = factory->get_num_global_factory();
    num_holder.store(static_cast<uint32_t>(num_local_factory));
    std::vector<std::vector<size_t>>& fids = all_factory;
    fids.resize(factory->get_num_machine());
    for (size_t i = 0; i < num_global_factory; i++) {
        fids[factory->get_machine_id(i)].push_back(i);
    }
    local_centers = fids[factory->get_machine_id()];
    global_centers.reserve(num_global_factory);
    // Complexity: O(num_machine * max(num_local_factory))
    // It can be reduced to O(num_global_factory) by sorting fids by size
    size_t max_size = 0;
    for (auto& fid : fids) {
        max_size = std::max(max_size, fid.size());
    }
    for (size_t i = 0; i < max_size; i++) {
        for (size_t j = 0; j < fids.size(); j++) {
            if (i < fids[j].size()) {
                global_centers.push_back(fids[j][i]);
            }
        }
    }
}

AggregatorFactoryBase::InnerSharedData::~InnerSharedData() {
    for (auto& i : global_aggs) {
        delete i;
    }
}

}  // namespace lib
}  // namespace husky
