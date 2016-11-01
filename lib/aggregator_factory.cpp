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

#include "lib/aggregator_factory.hpp"

#include <vector>

#include "base/serialization.hpp"
#include "core/accessor.hpp"
#include "core/channel/aggregator_channel.hpp"
#include "core/context.hpp"
#include "lib/aggregator.hpp"
#include "lib/aggregator_object.hpp"

namespace husky {
namespace lib {

using base::BinStream;
using base::CallOnceEachTime;

AggregatorChannel& AggregatorFactory::get_channel() { return static_cast<AggregatorFactory&>(get_factory()).channel(); }

AggregatorChannel& AggregatorFactory::channel() { return aggregator_channel_; }

size_t AggregatorFactory::get_factory_id() { return Context::get_global_tid(); }

size_t AggregatorFactory::get_num_global_factory() { return Context::get_worker_info()->get_num_workers(); }

size_t AggregatorFactory::get_num_local_factory() { return Context::get_worker_info()->get_num_local_workers(); }

size_t AggregatorFactory::get_num_machine() { return Context::get_worker_info()->get_num_processes(); }

size_t AggregatorFactory::get_machine_id() { return Context::get_worker_info()->get_proc_id(); }

size_t AggregatorFactory::get_machine_id(size_t fid) { return Context::get_worker_info()->get_proc_id(fid); }

void AggregatorFactory::send_local_update(std::vector<BinStream>& bins) { send(aggregator_channel_, bins); }

void AggregatorFactory::on_recv_local_update(const std::function<void(BinStream&)>& recv) {
    on_recv(aggregator_channel_, recv);
}

void AggregatorFactory::broadcast_aggregator(std::vector<BinStream>& bins) { send(aggregator_channel_, bins); }

void AggregatorFactory::on_recv_broadcast(const std::function<void(BinStream&)>& recv) {
    on_recv(aggregator_channel_, recv);
}

void AggregatorFactory::SharedData::initialize(AggregatorFactoryBase* factory) {
    AggregatorFactory::InnerSharedData::initialize(factory);
    size_t num_local_factory = static_cast<AggregatorFactory*>(factory)->get_num_local_factory();
    all_local_agg_accessor.resize(num_local_factory);
    for (auto& accessor : all_local_agg_accessor) {
        accessor.init(num_local_factory);
    }
}

void AggregatorFactory::on_access_other_local_update(
    const std::function<void(std::vector<AggregatorState*>&)>& update) {
    shared_->all_local_agg_accessor[get_local_factory_idx()].commit(get_local_aggs());
    for (size_t i = 0, sz = get_num_local_factory(); i < sz; ++i) {
        Accessor<std::vector<AggregatorState*>>& a = shared_->all_local_agg_accessor[i];
        update(a.access());
        a.leave();
    }
}

AggregatorFactory::InnerSharedData* AggregatorFactory::create_shared_data() { return new SharedData(); }

void AggregatorFactory::wait_for_others() { shared_->barrier.wait(get_num_local_factory()); }

void AggregatorFactory::call_once(const std::function<void()>& lambda) {
    static CallOnceEachTime call_once_each_time;
    call_once_each_time(lambda);
}

void AggregatorFactory::init_factory() {
    AggregatorFactoryBase::init_factory();
    shared_ = static_cast<SharedData*>(get_shared_data());
    aggregator_channel_.default_setup(std::bind(&AggregatorFactory::synchronize, this));
}

void AggregatorFactory::send(AggregatorChannel& channel, std::vector<BinStream>& bin) { channel.send(bin); }

void AggregatorFactory::on_recv(AggregatorChannel& channel, const std::function<void(BinStream&)>& recv) {
    while (channel.poll()) {
        BinStream bin = channel.recv();
        if (bin.size() != 0) {
            recv(bin);
        }
    }
}

}  // namespace lib
}  // namespace husky
