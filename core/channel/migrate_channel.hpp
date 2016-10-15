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
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist.hpp"
#include "core/worker_info.hpp"

namespace husky {

using base::BinStream;

template <typename ObjT>
class MigrateChannel : public ObjList2ObjListChannel<ObjT, ObjT> {
   public:
    MigrateChannel(ObjList<ObjT>* src, ObjList<ObjT>* dst) : ObjList2ObjListChannel<ObjT, ObjT>(src, dst) {
        this->src_ptr_->register_outchannel(this->channel_id_, this);
        this->dst_ptr_->register_inchannel(this->channel_id_, this);
    }

    ~MigrateChannel() override {
        this->src_ptr_->deregister_outchannel(this->channel_id_);
        this->dst_ptr_->deregister_inchannel(this->channel_id_);
    }

    MigrateChannel(const MigrateChannel&) = delete;
    MigrateChannel& operator=(const MigrateChannel&) = delete;

    MigrateChannel(MigrateChannel&&) = default;
    MigrateChannel& operator=(MigrateChannel&&) = default;

    void customized_setup() override { migrate_buffer_.resize(this->worker_info_->get_num_workers()); }

    void migrate(ObjT& obj, int dst_thread_id) {
        auto idx = this->src_ptr_->delete_object(&obj);
        migrate_buffer_[dst_thread_id] << obj;
        this->src_ptr_->migrate_attribute(migrate_buffer_[dst_thread_id], idx);
    }

    void prepare() override {}

    void in(BinStream& bin) override { process_bin(bin); }

    void out() override { flush(); }

    /// This method is only useful without list_execute
    void flush() {
        this->inc_progress();
        int start = this->global_id_;
        for (int i = 0; i < migrate_buffer_.size(); ++i) {
            int dst = (start + i) % migrate_buffer_.size();
            this->mailbox_->send(dst, this->channel_id_, this->progress_, migrate_buffer_[dst]);
            migrate_buffer_[dst].purge();
        }
        this->mailbox_->send_complete(this->channel_id_, this->progress_);
    }

    /// This method is only useful without list_execute
    void prepare_immigrants() {
        if (!this->is_flushed())
            return;
        // process immigrants
        while (this->mailbox_->poll(this->channel_id_, this->progress_)) {
            auto bin_push = this->mailbox_->recv(this->channel_id_, this->progress_);
            process_bin(bin_push);
        }
        // TODO(yuzhen): Should I put sort here or other place
        // object insertion finalize
        // dst_ptr->sort();
        this->reset_flushed();
    }

   protected:
    void process_bin(BinStream& bin_push) {
        while (bin_push.size() != 0) {
            ObjT obj;
            bin_push >> obj;
            auto idx = this->dst_ptr_->add_object(std::move(obj));
            this->dst_ptr_->process_attribute(bin_push, idx);
        }
    }

    std::vector<BinStream> migrate_buffer_;
};

}  // namespace husky
