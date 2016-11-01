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

#include <vector>

#include "base/serialization.hpp"
#include "base/thread_support.hpp"
#include "core/accessor.hpp"
#include "core/channel/aggregator_channel.hpp"
#include "lib/aggregator.hpp"
#include "lib/aggregator_object.hpp"

namespace husky {
namespace lib {

using base::BinStream;
using base::KBarrier;

class AggregatorFactory : public AggregatorFactoryBase {
   public:
    static AggregatorChannel& get_channel();

   protected:
    AggregatorChannel& channel();
    virtual size_t get_factory_id();
    virtual size_t get_num_global_factory();
    virtual size_t get_num_local_factory();
    virtual size_t get_num_machine();
    virtual size_t get_machine_id();
    virtual size_t get_machine_id(size_t fid);
    virtual void send_local_update(std::vector<BinStream>& bins);
    virtual void on_recv_local_update(const std::function<void(BinStream&)>& recv);
    virtual void broadcast_aggregator(std::vector<BinStream>& bins);
    virtual void on_recv_broadcast(const std::function<void(BinStream&)>& recv);

    class SharedData : public InnerSharedData {
       public:
        virtual void initialize(AggregatorFactoryBase* factory);
        KBarrier barrier;
        std::vector<Accessor<std::vector<AggregatorState*>>> all_local_agg_accessor;
    } * shared_{nullptr};

    virtual void on_access_other_local_update(const std::function<void(std::vector<AggregatorState*>&)>& update);
    virtual InnerSharedData* create_shared_data();
    virtual void wait_for_others();
    virtual void call_once(const std::function<void()>& lambda);
    virtual void init_factory();

   private:
    void send(AggregatorChannel& channel, std::vector<BinStream>& bin);
    void on_recv(AggregatorChannel& channel, const std::function<void(BinStream&)>& recv);
    AggregatorChannel aggregator_channel_;
};

}  // namespace lib
}  // namespace husky
