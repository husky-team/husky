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
#include "core/accessor_store.hpp"
#include "core/channel/channel_base.hpp"
#include "core/channel/channel_source.hpp"
#include "core/combiner.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist.hpp"
#include "core/worker_info.hpp"

namespace husky {

using base::BinStream;

template <typename KeyT, typename ValueT>
class BroadcastChannel : public ChannelBase {
   public:
    explicit BroadcastChannel(ChannelSource* src) : src_ptr_(src) {
        // TODO(yuzhen): Should be careful, maybe need to deregister every time?
        src_ptr_->register_outchannel(channel_id_, this);
    }

    ~BroadcastChannel() override {
        // Make sure to invoke inc_progress_ before destructor
        if (need_leave_accessor_)
            leave_accessor();
        AccessorStore::remove_accessor(channel_id_);
        src_ptr_->deregister_outchannel(channel_id_);
    }

    BroadcastChannel(const BroadcastChannel&) = delete;
    BroadcastChannel& operator=(const BroadcastChannel&) = delete;

    BroadcastChannel(BroadcastChannel&&) = default;
    BroadcastChannel& operator=(BroadcastChannel&&) = default;

    void customized_setup() override {
        broadcast_buffer_.resize(worker_info_->get_num_workers());
        accessor_ = AccessorStore::create_accessor<std::unordered_map<KeyT, ValueT>>(
            channel_id_, local_id_, worker_info_->get_num_local_workers());
    }

    void broadcast(const KeyT& key, const ValueT& value) {
        for (int i = 0; i < worker_info_->get_num_processes(); ++i) {
            int recv_proc_num_worker = worker_info_->get_num_local_workers(i);
            int recver_local_id_ = std::hash<KeyT>()(key) % recv_proc_num_worker;
            int recver_id = worker_info_->local_to_global_id(i, recver_local_id_);
            broadcast_buffer_[recver_id] << key << value;
        }
    }

    ValueT& get(const KeyT& key) {
        auto& dict = (*accessor_)[std::hash<KeyT>()(key) % worker_info_->get_num_local_workers()].access();
        auto iter = dict.find(key);
        ASSERT_MSG(iter != dict.end(), "Key Not Found");
        return iter->second;
    }

    bool get(const KeyT& key, ValueT* value) {
        auto& dict = (*accessor_)[std::hash<KeyT>()(key) % worker_info_->get_num_local_workers()].access();
        auto iter = dict.find(key);
        if (iter == dict.end())
            return false;
        *value = iter->second;
        return true;
    }

    bool find(const KeyT& key) {
        auto& dict = (*accessor_)[std::hash<KeyT>()(key) % worker_info_->get_num_local_workers()].access();
        auto iter = dict.find(key);
        return iter != dict.end();
    }

    void set_clear_dict(bool clear) { clear_dict_each_progress_ = clear; }

    void prepare() override {}

    void in(BinStream& bin) override {}

    void out() override {
        flush();
        prepare_broadcast();
    }

    /// This method is only useful without list_execute
    void flush() {
        this->inc_progress();
        int start = global_id_;
        for (int i = 0; i < broadcast_buffer_.size(); ++i) {
            int dst = (start + i) % broadcast_buffer_.size();
            if (broadcast_buffer_[dst].size() == 0)
                continue;
            mailbox_->send(dst, channel_id_, progress_, broadcast_buffer_[dst]);
            broadcast_buffer_[dst].purge();
        }
        mailbox_->send_complete(this->channel_id_, this->progress_, this->hash_ring_);
    }

    /// This method is only useful without list_execute
    void prepare_broadcast() {
        // Check whether need to leave accessor_ (last round's accessor_)
        if (need_leave_accessor_)
            leave_accessor();
        need_leave_accessor_ = true;

        auto& local_dict = (*accessor_)[local_id_].storage();
        while (mailbox_->poll(channel_id_, progress_)) {
            auto bin = mailbox_->recv(channel_id_, progress_);
            process_bin(bin, local_dict);
        }
        (*accessor_)[local_id_].commit();
    }

   protected:
    void leave_accessor() {
        for (int i = 0; i < worker_info_->get_num_local_workers(); ++i)
            (*accessor_)[i].leave();

        if (clear_dict_each_progress_) {
            (*accessor_)[local_id_].storage().clear();
            (*accessor_)[local_id_].commit();
            for (int i = 0; i < worker_info_->get_num_local_workers(); ++i)
                (*accessor_)[i].leave();
        }
    }

    void process_bin(BinStream& bin, std::unordered_map<KeyT, ValueT>& local_dict) {
        while (bin.size() != 0) {
            KeyT key;
            ValueT value;
            bin >> key >> value;
            local_dict[key] = value;
        }
    }

    ChannelSource* src_ptr_;

    bool clear_dict_each_progress_ = false;
    bool need_leave_accessor_ = false;
    std::vector<BinStream> broadcast_buffer_;
    std::vector<Accessor<std::unordered_map<KeyT, ValueT>>>* accessor_;
};

}  // namespace husky
