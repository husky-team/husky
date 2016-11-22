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

#include <chrono>
#include <iostream>
#include <vector>

#include "base/exception.hpp"
#include "base/log.hpp"
#include "core/channel/channel_base.hpp"
#include "core/channel/channel_manager.hpp"
#include "core/channel/channel_store.hpp"
#include "core/context.hpp"
#include "core/objlist.hpp"

namespace husky {

template <typename ObjT>
void globalize(ObjList<ObjT>& obj_list) {
    // create a migrate channel for globalize
    auto& migrate_channel = ChannelStore::create_migrate_channel(obj_list, obj_list, "tmp_globalize");

    for (auto& obj : obj_list.get_data()) {
        int dst_thread_id = obj_list.get_hash_ring().hash_lookup(obj.id());
        if (dst_thread_id != Context::get_global_tid()) {
            migrate_channel.migrate(obj, dst_thread_id);
        }
    }
    obj_list.deletion_finalize();
    migrate_channel.flush();
    migrate_channel.prepare_immigrants();
    obj_list.sort();

    ChannelStore::drop_channel("tmp_globalize");
    // TODO(all): Maybe we can skip using unordered_map to index obj since in the end we need to sort them
}

/// Warning: can only handle async push and async migrate
/// Channel must be AsyncPushChannel or AsyncMigrateChannel
/// Only one channel is allowed so far
/// TODO(Wei): Multiple channels should be allowed to bind to one object list
template <typename ObjT, typename ExecT>
void list_execute_async(ObjList<ObjT>& obj_list, ExecT execute, int async_time, double timeout = 0.0) {
    std::vector<ChannelBase*> channels = obj_list.get_inchannels();
    if (channels.size() != 1)
        throw base::HuskyException("list_execute_async currently only supports exactly one channel.");
    ChannelBase* channel = channels[0];
    if (channel->get_channel_type() != ChannelBase::ChannelType::Async)
        throw base::HuskyException("list_execute_async currently only supports one asynchronous channel.");

    auto start = std::chrono::steady_clock::now();
    auto duration = std::chrono::seconds(async_time);
    auto* mailbox = channel->get_mailbox();
    while (std::chrono::steady_clock::now() - start < duration) {
        // 1. receive messages if any
        channel->prepare();
        if (timeout == 0.0) {
            while (mailbox->poll_non_block(channel->get_channel_id(), channel->get_progress())) {
                auto bin = mailbox->recv(channel->get_channel_id(), channel->get_progress());
                channel->in(bin);
            }
        } else {
            while (mailbox->poll_with_timeout(channel->get_channel_id(), channel->get_progress(), timeout)) {
                auto bin = mailbox->recv(channel->get_channel_id(), channel->get_progress());
                channel->in(bin);
            }
        }

        // 2. iterate over the list
        for (size_t i = 0; i < obj_list.get_vector_size(); ++i) {
            if (obj_list.get_del(i))
                continue;
            execute(obj_list.get(i));
        }

        // 3. flush
        channel->out();
    }
    mailbox->send_complete(channel->get_channel_id(), channel->get_progress(), Context::get_hashring());
    channel->prepare();
    while (mailbox->poll(channel->get_channel_id(), channel->get_progress())) {
        auto bin = mailbox->recv(channel->get_channel_id(), channel->get_progress());
        channel->in(bin);
    }
    channel->inc_progress();
}

template <typename ObjT, typename ExecT>
void list_execute(ObjList<ObjT>& obj_list, ExecT execute) {
    // TODO(all): the order of invoking prefuncs may matter.
    // e.g. MigrateChannel should be invoked before PushChannel
    ChannelManager in_manager(obj_list.get_inchannels());
    in_manager.poll_and_distribute();

    for (size_t i = 0; i < obj_list.get_vector_size(); ++i) {
        if (obj_list.get_del(i))
            continue;
        execute(obj_list.get(i));
    }

    ChannelManager out_manager(obj_list.get_outchannels());
    out_manager.flush();
}

template <typename ObjT, typename ExecT>
void list_execute(ObjList<ObjT>& obj_list, const std::vector<ChannelBase*>& in_channel,
                  const std::vector<ChannelBase*>& out_channel, ExecT execute) {
    // TODO(all): the order of invoking prefuncs may matter.
    // e.g. MigrateChannel should be invoked before PushChannel
    ChannelManager in_manager(in_channel);
    in_manager.poll_and_distribute();

    for (size_t i = 0; i < obj_list.get_vector_size(); ++i) {
        if (obj_list.get_del(i))
            continue;
        execute(obj_list.get(i));
    }

    ChannelManager out_manager(out_channel);
    out_manager.flush();
}

template <typename InputFormatT, typename ParseT>
void load(InputFormatT& infmt, const ParseT& parse) {
    ASSERT_MSG(infmt.is_setup(), "InputFormat has not been setup.");

    typename InputFormatT::RecordT record;
    bool success = false;
    while (true) {
        success = infmt.next(record);
        if (success == false)
            break;
        parse(InputFormatT::recast(record));
    }

    ChannelManager out_manager(infmt.get_outchannels());
    out_manager.flush();
}

template <typename InputFormatT, typename ParseT>
void load(InputFormatT& infmt, const std::vector<ChannelBase*>& out_channel, const ParseT& parse) {
    ASSERT_MSG(infmt.is_setup(), "InputFormat has not been setup.");

    typename InputFormatT::RecordT record;
    bool success = false;
    while (true) {
        success = infmt.next(record);
        if (success == false)
            break;
        parse(InputFormatT::recast(record));
    }

    ChannelManager out_manager(out_channel);
    out_manager.flush();
}

}  // namespace husky
