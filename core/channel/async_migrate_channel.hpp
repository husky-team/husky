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
#include <vector>

#include "base/serialization.hpp"
#include "core/channel/channel_impl.hpp"
#include "core/channel/migrate_channel.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist.hpp"
#include "core/worker_info.hpp"

namespace husky {

using base::BinStream;

template <typename ObjT>
class AsyncMigrateChannel : public MigrateChannel<ObjT> {
   public:
    explicit AsyncMigrateChannel(ObjList<ObjT>* objlist) : MigrateChannel<ObjT>(objlist, objlist) {
        this->set_as_async_channel();
    }

    AsyncMigrateChannel(const AsyncMigrateChannel&) = delete;
    AsyncMigrateChannel& operator=(const AsyncMigrateChannel&) = delete;

    AsyncMigrateChannel(AsyncMigrateChannel&&) = default;
    AsyncMigrateChannel& operator=(AsyncMigrateChannel&&) = default;

    /// This method is only useful without list_execute
    void out() override {
        // No increment progress id here
        int start = this->global_id_;
        for (int i = 0; i < this->migrate_buffer_.size(); ++i) {
            int dst = (start + i) % this->migrate_buffer_.size();
            this->mailbox_->send(dst, this->channel_id_, this->progress_, this->migrate_buffer_[dst]);
            this->migrate_buffer_[dst].purge();
        }
        // No send_complete here
    }
};

}  // namespace husky
