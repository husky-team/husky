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

#include "core/channel/channel_base.hpp"

#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/worker_info.hpp"

namespace husky {

thread_local size_t ChannelBase::counter = 0;

ChannelBase::ChannelBase() : channel_id_(counter), progress_(0) {
    counter += 1;
    set_as_sync_channel();
}

void ChannelBase::set_local_id(size_t local_id) { local_id_ = local_id; }

void ChannelBase::set_global_id(size_t global_id) { global_id_ = global_id; }

void ChannelBase::set_worker_info(WorkerInfo* worker_info) { worker_info_ = worker_info; }

void ChannelBase::set_mailbox(LocalMailbox* mailbox) { mailbox_ = mailbox; }

void ChannelBase::set_hash_ring(HashRing* hash_ring) { hash_ring_ = hash_ring; }

void ChannelBase::set_as_async_channel() { type_ = ChannelType::Async; }

void ChannelBase::set_as_sync_channel() { type_ = ChannelType::Sync; }

void ChannelBase::setup(size_t local_id, size_t global_id, WorkerInfo* worker_info, LocalMailbox* mailbox,
                        HashRing* hash_ring) {
    set_local_id(local_id);
    set_global_id(global_id);
    set_worker_info(worker_info);
    set_mailbox(mailbox);
    set_hash_ring(hash_ring);
    customized_setup();
}

void ChannelBase::inc_progress() {
    progress_ += 1;
    flushed_.resize(progress_ + 1, true);
}

}  // namespace husky
