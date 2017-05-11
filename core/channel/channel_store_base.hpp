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

#include <string>
#include <unordered_map>

#include "base/exception.hpp"
#include "core/channel/async_migrate_channel.hpp"
#include "core/channel/async_push_channel.hpp"
#include "core/channel/broadcast_channel.hpp"
#include "core/channel/channel_base.hpp"
#include "core/channel/migrate_channel.hpp"
#include "core/channel/push_channel.hpp"
#include "core/channel/push_combined_channel.hpp"

namespace husky {

typedef std::unordered_map<size_t, ChannelBase*> ChannelIdMap;
typedef std::unordered_map<std::string, ChannelBase*> ChannelNameMap;

class ChannelStoreBase {
   public:
    template <typename MsgT, typename DstObjT>
    static PushChannel<MsgT, DstObjT>& create_push_channel(ChannelSource& src_list, ObjList<DstObjT>& dst_list) {
        ChannelIdMap& channel_map = get_channel_id_map();
        auto* push_channel = new PushChannel<MsgT, DstObjT>(&src_list, &dst_list);
        channel_map.insert({push_channel->get_channel_id(), push_channel});
        return *push_channel;
    }

    template <typename MsgT, typename DstObjT>
    static PushChannel<MsgT, DstObjT>& create_push_channel(ChannelSource& src_list, ObjList<DstObjT>& dst_list, const std::string& channel_name) {
        if (has_channel(channel_name)) {
            throw base::HuskyException("A Channel named "+channel_name+" already exists.");
        }
        auto& channel = create_push_channel<MsgT, DstObjT>(src_list, dst_list);
        get_channel_name_map().insert({channel_name, &channel});
        return channel;
    }

    template <typename MsgT, typename DstObjT>
    static PushChannel<MsgT, DstObjT>& get_push_channel(const std::string& channel_name) {
        return *(static_cast<PushChannel<MsgT, DstObjT>*>(get_channel_name_map().at(channel_name)));
    }

    template <typename MsgT, typename CombineT, typename DstObjT>
    static PushCombinedChannel<MsgT, DstObjT, CombineT>& create_push_combined_channel(ChannelSource& src_list,
                                                                                      ObjList<DstObjT>& dst_list) {
        ChannelIdMap& channel_map = get_channel_map();
        auto* push_combined_channel = new PushCombinedChannel<MsgT, DstObjT, CombineT>(&src_list, &dst_list);
        channel_map.insert({push_combined_channel->get_channel_id(), push_combined_channel});
        return *push_combined_channel;
    }

    template <typename MsgT, typename CombineT, typename DstObjT>
    static PushCombinedChannel<MsgT, DstObjT, CombineT>& create_push_combined_channel(ChannelSource& src_list,
                                                                                      ObjList<DstObjT>& dst_list,
                                                                                      const std::string& channel_name) {
        if (has_channel(channel_name)) {
            throw base::HuskyException("A Channel named "+channel_name+" already exists.");
        }
        auto& channel = create_push_combined_channel<MsgT, CombineT, DstObjT>(src_list, dst_list);
        get_channel_name_map().insert({channel_name, &channel});
        return channel;
    }

    template <typename MsgT, typename CombineT, typename DstObjT>
    static PushCombinedChannel<MsgT, DstObjT, CombineT>& get_push_combined_channel(const std::string& channel_name) {
        return *(static_cast<PushCombinedChannel<MsgT, DstObjT, CombineT>*>(get_channel_name_map().at(channel_name)));
    }

    template <typename ObjT>
    static MigrateChannel<ObjT>& create_migrate_channel(ObjList<ObjT>& src_list, ObjList<ObjT>& dst_list) {
        ChannelIdMap& channel_map = get_channel_map();
        auto* migrate_channel = new MigrateChannel<ObjT>(&src_list, &dst_list);
        channel_map.insert({migrate_channel->get_channel_id(), migrate_channel});
        return *migrate_channel;
    }

    template <typename KeyT, typename MsgT>
    static BroadcastChannel<KeyT, MsgT>& create_broadcast_channel(ChannelSource& src_list) {
        ChannelIdMap& channel_map = get_channel_map();
        auto* broadcast_channel = new BroadcastChannel<KeyT, MsgT>(&src_list);
        channel_map.insert({broadcast_channel->get_channel_id(), broadcast_channel});
        return *broadcast_channel;
    }

    template <typename MsgT, typename ObjT>
    static AsyncPushChannel<MsgT, ObjT>& create_async_push_channel(ObjList<ObjT>& obj_list) {
        ChannelIdMap& channel_map = get_channel_map();
        auto* async_push_channel = new AsyncPushChannel<MsgT, ObjT>(&obj_list);
        channel_map.insert({async_push_channel->get_channel_id(), async_push_channel});
        return *async_push_channel;
    }

    template <typename ObjT>
    static AsyncMigrateChannel<ObjT>& create_async_migrate_channel(ObjList<ObjT>& obj_list) {
        ChannelIdMap& channel_map = get_channel_map();
        auto* async_migrate_channel = new AsyncMigrateChannel<ObjT>(&obj_list);
        channel_map.insert({async_migrate_channel->get_channel_id(), async_migrate_channel});
        return *async_migrate_channel;
    }

    static bool has_channel(const size_t id);
    static bool has_channel(const std::string& channel_name);
    static void drop_channel(const size_t id);
    static void drop_channel(const std::string& channel_name);
    static void drop_all_channels();

    static void init_channel_map();
    static void free_channel_map();
    static ChannelIdMap& get_channel_map();
    static ChannelIdMap& get_channel_id_map();
    static ChannelNameMap& get_channel_name_map();

    static inline size_t size() {
        if (s_channel_id_map == nullptr)
            return 0;
        return s_channel_id_map->size();
    }

   protected:
    static thread_local ChannelIdMap* s_channel_id_map;
    static thread_local ChannelNameMap* s_channel_name_map;
};

}  // namespace husky
