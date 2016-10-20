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

#include "core/channel/aggregator_channel.hpp"

#include <functional>
#include <vector>

#include "base/log.hpp"
#include "base/serialization.hpp"
#include "core/context.hpp"
#include "core/mailbox.hpp"
#include "core/utils.hpp"

namespace husky {

AggregatorChannel::AggregatorChannel() {}

AggregatorChannel::~AggregatorChannel() {}

void AggregatorChannel::default_setup(std::function<void()> something = nullptr) {
    do_something = something;
    setup(Context::get_local_tid(), Context::get_global_tid(), Context::get_worker_info(), Context::get_mailbox(),
          Context::get_hashring());
}

// Mark these member function private to avoid being used by users
void AggregatorChannel::send(std::vector<BinStream>& bins) {
    this->inc_progress();
    ASSERT_MSG(bins.size() == this->worker_info_->get_num_workers(),
               ("Number of messages to send is expected to be number of workers,"
                " expected: " +
                std::to_string(this->worker_info_->get_num_workers()) + ", in fact: " + std::to_string(bins.size()))
                   .c_str());
    for (int i = 0; i < bins.size(); i++) {
        if (bins[i].size() > 0) {
            this->mailbox_->send(i, this->channel_id_, this->progress_, bins[i]);
            bins[i].clear();
        }
    }
    this->mailbox_->send_complete(this->channel_id_, this->progress_, this->hash_ring_);
}

bool AggregatorChannel::poll() { return this->mailbox_->poll(this->channel_id_, this->progress_); }

BinStream AggregatorChannel::recv() { return this->mailbox_->recv(this->channel_id_, this->progress_); }

void AggregatorChannel::prepare() {}

void AggregatorChannel::in(BinStream& bin) {}

void AggregatorChannel::out() {
    if (do_something != nullptr)
        do_something();
}

void AggregatorChannel::customized_setup() {}

}  // namespace husky
