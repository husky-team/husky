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

#include <time.h>

#include <functional>
#include <utility>
#include <vector>

#include "base/serialization.hpp"
#include "core/channel/channel_base.hpp"
#include "core/channel/channel_impl.hpp"
#include "core/combiner.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist.hpp"
#include "core/shuffle_combiner_store.hpp"
#include "core/worker_info.hpp"

namespace husky {

using base::BinStream;

template <typename MsgT, typename DstObjT, typename CombineT>
class PushCombinedChannel : public Source2ObjListChannel<DstObjT> {
   public:
    PushCombinedChannel(ChannelSource* src, ObjList<DstObjT>* dst) : Source2ObjListChannel<DstObjT>(src, dst) {
        this->src_ptr_->register_outchannel(this->channel_id_, this);
        this->dst_ptr_->register_inchannel(this->channel_id_, this);
    }

    ~PushCombinedChannel() override {
        ShuffleCombinerStore::remove_shuffle_combiner<typename DstObjT::KeyT, MsgT>(this->channel_id_);

        this->src_ptr_->deregister_outchannel(this->channel_id_);
        this->dst_ptr_->deregister_inchannel(this->channel_id_);
    }

    PushCombinedChannel(const PushCombinedChannel&) = delete;
    PushCombinedChannel& operator=(const PushCombinedChannel&) = delete;

    PushCombinedChannel(PushCombinedChannel&&) = default;
    PushCombinedChannel& operator=(PushCombinedChannel&&) = default;

    void customized_setup() override {
        // Initialize send_buffer_
        // use get_largest_tid() instead of get_num_workers()
        // sine we may only use a subset of worker
        send_buffer_.resize(this->worker_info_->get_largest_tid() + 1);
        // Create shuffle_combiner_
        // TODO(yuzhen): Only support sortcombine, hashcombine can be added using enableif
        shuffle_combiner_ = ShuffleCombinerStore::create_shuffle_combiner<typename DstObjT::KeyT, MsgT>(
            this->channel_id_, this->local_id_, this->worker_info_->get_num_local_workers(),
            this->worker_info_->get_largest_tid() + 1);
    }

    void push(const MsgT& msg, const typename DstObjT::KeyT& key) {
        // shuffle_combiner_.init();  // Already move init() to create_shuffle_combiner_()
        int dst_worker_id = this->worker_info_->get_hash_ring().hash_lookup(key);
        auto& buffer = (*shuffle_combiner_)[this->local_id_].storage(dst_worker_id);
        back_combine<CombineT>(buffer, key, msg);
    }

    const MsgT& get(const DstObjT& obj) {
        auto idx = this->dst_ptr_->index_of(&obj);
        if (idx >= recv_buffer_.size()) {  // resize recv_buffer_ and recv_flag_ if it is not large enough
            recv_buffer_.resize(this->dst_ptr_->get_size());
            recv_flag_.resize(this->dst_ptr_->get_size());
        }
        if (recv_flag_[idx] == false) {
            recv_buffer_[idx] = MsgT();
        }
        return recv_buffer_[idx];
    }

    bool has_msgs(const DstObjT& obj) {
        auto idx = this->dst_ptr_->index_of(&obj);
        if (idx >= recv_buffer_.size()) {  // resize recv_buffer_ and recv_flag_ if it is not large enough
            recv_buffer_.resize(this->dst_ptr_->get_size());
            recv_flag_.resize(this->dst_ptr_->get_size());
        }
        return recv_flag_[idx];
    }

    void prepare() override { clear_recv_buffer_(); }

    void in(BinStream& bin) override { process_bin(bin); }

    void out() override { flush(); }

    void send() {
        int start = this->global_id_;
        for (int i = 0; i < send_buffer_.size(); ++i) {
            int dst = (start + i) % send_buffer_.size();
            if (send_buffer_[dst].size() == 0)
                continue;
            this->mailbox_->send(dst, this->channel_id_, this->progress_+1, send_buffer_[dst]);
            send_buffer_[dst].purge();
        }
    }

    void send_complete() {
        this->inc_progress();
        this->mailbox_->send_complete(this->channel_id_, this->progress_, this->worker_info_->get_local_tids(),
                                      this->worker_info_->get_pids());
    }

    /// This method is only useful without list_execute
    void flush() {
        shuffle_combine();
        send();
        send_complete();
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

    ShuffleCombiner<std::pair<typename DstObjT::KeyT, MsgT>>& get_shuffle_combiner(int tid) {
        return (*shuffle_combiner_)[tid];
    }   

    std::vector<BinStream>& get_send_buffer() {
        return send_buffer_;
    }

   protected:
    void clear_recv_buffer_() { std::fill(recv_flag_.begin(), recv_flag_.end(), false); }

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
            if (idx >= recv_buffer_.size()) {
                recv_buffer_.resize(idx + 1);
                recv_flag_.resize(idx + 1);
            }
            if (recv_flag_[idx] == true) {
                CombineT::combine(recv_buffer_[idx], msg);
            } else {
                recv_buffer_[idx] = std::move(msg);
                recv_flag_[idx] = true;
            }
        }
    }

    void shuffle_combine() {
        // step 1: shuffle combine
        auto& self_shuffle_combiner = (*shuffle_combiner_)[this->local_id_];
        self_shuffle_combiner.send_shuffler_buffer();
        for (int iter = 0; iter < this->worker_info_->get_num_local_workers() - 1; iter++) {
            int next_worker = self_shuffle_combiner.access_next();
            auto& peer_shuffle_combiner = (*shuffle_combiner_)[next_worker];
            for (int i = this->local_id_; i < this->worker_info_->get_largest_tid() + 1;
                 i += this->worker_info_->get_num_local_workers()) {
                // combining the i-th buffer
                auto& self_buffer = self_shuffle_combiner.storage(i);
                auto& peer_buffer = peer_shuffle_combiner.storage(i);
                self_buffer.insert(self_buffer.end(), peer_buffer.begin(), peer_buffer.end());
                peer_buffer.clear();
            }
        }
        for (int i = this->local_id_; i < this->worker_info_->get_largest_tid() + 1;
             i += this->worker_info_->get_num_local_workers()) {
            auto& self_buffer = self_shuffle_combiner.storage(i);
            combine_single<CombineT>(self_buffer);
        }
        // step 2: serialize combine buffer
        for (int i = this->local_id_; i < this->worker_info_->get_largest_tid() + 1;
             i += this->worker_info_->get_num_local_workers()) {
            auto& combine_buffer = self_shuffle_combiner.storage(i);
            for (int k = 0; k < combine_buffer.size(); k++) {
                send_buffer_[i] << combine_buffer[k].first;
                send_buffer_[i] << combine_buffer[k].second;
            }
            combine_buffer.clear();
        }
    }

    std::vector<ShuffleCombiner<std::pair<typename DstObjT::KeyT, MsgT>>>* shuffle_combiner_;
    std::vector<BinStream> send_buffer_;
    std::vector<MsgT> recv_buffer_;
    std::vector<bool> recv_flag_;
};

}  // namespace husky
