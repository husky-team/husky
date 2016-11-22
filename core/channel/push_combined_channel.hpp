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
        ShuffleCombinerStore::remove_shuffle_combiner(this->channel_id_);

        this->src_ptr_->deregister_outchannel(this->channel_id_);
        this->dst_ptr_->deregister_inchannel(this->channel_id_);
    }

    PushCombinedChannel(const PushCombinedChannel&) = delete;
    PushCombinedChannel& operator=(const PushCombinedChannel&) = delete;

    PushCombinedChannel(PushCombinedChannel&&) = default;
    PushCombinedChannel& operator=(PushCombinedChannel&&) = default;

    void customized_setup() override {
        // Initialize send_buffer_
        send_buffer_.resize(this->worker_info_->get_num_workers());
        // Create shuffle_combiner_
        // TODO(yuzhen): Only support sortcombine, hashcombine can be added using enableif
        shuffle_combiner_ = ShuffleCombinerStore::create_shuffle_combiner<typename DstObjT::KeyT, MsgT>(
            this->channel_id_, this->local_id_, this->worker_info_->get_num_local_workers(),
            this->worker_info_->get_num_workers());
    }

    void push(const MsgT& msg, const typename DstObjT::KeyT& key) {
        // shuffle_combiner_.init();  // Already move init() to create_shuffle_combiner_()
        int dst_worker_id = this->hash_ring_->hash_lookup(key);
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

    /// This method is only useful without list_execute
    void flush() {
        this->inc_progress();
        shuffle_combine();

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
        for (int i = 0; i < this->worker_info_->get_num_workers(); i++)
            (*shuffle_combiner_)[this->local_id_].commit(i);

        for (int i = 0; i < this->worker_info_->get_num_workers(); i++) {
            if (i % this->worker_info_->get_num_local_workers() != this->local_id_)
                continue;

            // assume I'm now combining the i-th buffer
            auto& self_buffer = (*shuffle_combiner_)[this->local_id_].access(i);

            // Collect messages (of the i-th buffer) from all machines
            unsigned int seed = time(NULL);
            int base = rand_r(&seed);
            for (int j = 0; j < this->worker_info_->get_num_local_workers(); j++) {
                if (j == this->local_id_)
                    continue;

                auto& peer_buffer = (*shuffle_combiner_)[j].access(i);
                self_buffer.insert(self_buffer.end(), peer_buffer.begin(), peer_buffer.end());
                peer_buffer.clear();
                (*shuffle_combiner_)[j].leave(i);
            }

            combine_single<CombineT>(self_buffer);
        }

        // step 2: serialize combine buffer
        for (int i = 0; i < this->worker_info_->get_num_workers(); i++) {
            if (i % this->worker_info_->get_num_local_workers() != this->local_id_)
                continue;

            auto& combine_buffer = (*shuffle_combiner_)[this->local_id_].access(i);
            for (int k = 0; k < combine_buffer.size(); k++) {
                send_buffer_[i] << combine_buffer[k].first;
                send_buffer_[i] << combine_buffer[k].second;
            }
            combine_buffer.clear();
            (*shuffle_combiner_)[this->local_id_].leave(i);
        }
    }

    std::vector<ShuffleCombiner<std::pair<typename DstObjT::KeyT, MsgT>>>* shuffle_combiner_;
    std::vector<BinStream> send_buffer_;
    std::vector<MsgT> recv_buffer_;
    std::vector<bool> recv_flag_;
};

}  // namespace husky
