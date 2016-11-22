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

template <typename MsgT, typename DstObjT>
class PushChannel : public Source2ObjListChannel<DstObjT> {
   public:
    PushChannel(ChannelSource* src, ObjList<DstObjT>* dst) : Source2ObjListChannel<DstObjT>(src, dst) {
        this->src_ptr_->register_outchannel(this->channel_id_, this);
        this->dst_ptr_->register_inchannel(this->channel_id_, this);
    }

    ~PushChannel() override {
        if (this->src_ptr_ != nullptr)
            this->src_ptr_->deregister_outchannel(this->channel_id_);
        if (this->dst_ptr_ != nullptr)
            this->dst_ptr_->deregister_inchannel(this->channel_id_);
    }
    PushChannel(const PushChannel&) = delete;
    PushChannel& operator=(const PushChannel&) = delete;

    PushChannel(PushChannel&&) = default;
    PushChannel& operator=(PushChannel&&) = default;

    void customized_setup() override { send_buffer_.resize(this->worker_info_->get_num_workers()); }

    void push(const MsgT& msg, const typename DstObjT::KeyT& key) {
        int dst_worker_id = this->hash_ring_->hash_lookup(key);
        send_buffer_[dst_worker_id] << key << msg;
    }

    const std::vector<MsgT>& get(const DstObjT& obj) {
        auto idx = this->dst_ptr_->index_of(&obj);
        if (idx >= recv_buffer_.size()) {  // resize recv_buffer_ if it is not large enough
            recv_buffer_.resize(this->dst_ptr_->get_size());
        }
        return recv_buffer_[idx];
    }

    void prepare() override { clear_recv_buffer_(); }

    void in(BinStream& bin) override { process_bin(bin); }

    void out() override { flush(); }

    /// This method is only useful without list_execute
    void flush() {
        this->inc_progress();
        int start = this->global_id_;
        for (int i = 0; i < send_buffer_.size(); ++i) {
            int dst = (start + i) % send_buffer_.size();
            if (send_buffer_[dst].size() == 0)
                continue;
            this->mailbox_->send(dst, this->channel_id_, this->progress_, send_buffer_[dst]);
            send_buffer_[dst].purge();
        }
        this->mailbox_->send_complete(this->channel_id_, this->progress_, this->hash_ring_);
    }

    /// This method is only useful without list_execute
    void prepare_messages() {
        if (!this->is_flushed())
            return;
        clear_recv_buffer_();
        while (this->mailbox_->poll(this->channel_id_, this->progress_)) {
            auto bin_push = this->mailbox_->recv(this->channel_id_, this->progress_);
            process_bin(bin_push);
        }
        this->reset_flushed();
    }

   protected:
    void clear_recv_buffer_() {
        // TODO(yuzhen): What types of clear do we need?
        for (auto& vec : recv_buffer_)
            vec.clear();
    }
    void process_bin(BinStream& bin_push) {
        while (bin_push.size() != 0) {
            typename DstObjT::KeyT key;
            bin_push >> key;
            MsgT msg;
            bin_push >> msg;

            DstObjT* recver_obj = this->dst_ptr_->find(key);
            size_t idx;
            if (recver_obj == nullptr) {
                DstObjT obj(key);  // Construct obj using key only
                idx = this->dst_ptr_->add_object(std::move(obj));
            } else {
                idx = this->dst_ptr_->index_of(recver_obj);
            }
            if (idx >= recv_buffer_.size())
                recv_buffer_.resize(idx + 1);
            recv_buffer_[idx].push_back(std::move(msg));
        }
    }

    std::vector<BinStream> send_buffer_;
    std::vector<std::vector<MsgT>> recv_buffer_;
};

}  // namespace husky
