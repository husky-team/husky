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

#include "core/channel/async_migrate_channel.hpp"
#include "core/channel/async_push_channel.hpp"
#include "core/channel/broadcast_channel.hpp"
#include "core/channel/channel_base.hpp"
#include "core/channel/migrate_channel.hpp"
#include "core/channel/push_channel.hpp"
#include "core/channel/push_combined_channel.hpp"

namespace husky {

/// 3 types of APIs are provided
/// create_xxx_channel(), get_xxx_channel(), drop_channel()
class ChannelFactoryBase {
   public:
    // Create PushChannel
    template <typename MsgT, typename DstObjT>
    static PushChannel<MsgT, DstObjT>& create_push_channel(ChannelSource& src_list, ObjList<DstObjT>& dst_list,
                                                           const std::string& name = "") {
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(default_channel_id++) : name;
        ASSERT_MSG(channel_map.find(channel_name) == channel_map.end(),
                   "ChannelFactoryBase::create_channel: Channel name already exists");
        auto* push_channel = new PushChannel<MsgT, DstObjT>(&src_list, &dst_list);
        channel_map.insert({channel_name, push_channel});
        return *push_channel;
    }

    template <typename MsgT, typename DstObjT>
    static PushChannel<MsgT, DstObjT>& get_push_channel(const std::string& name = "") {
        ASSERT_MSG(channel_map.find(name) != channel_map.end(),
                   "ChannelFactoryBase::get_channel: Channel name doesn't exist");
        auto* channel = channel_map[name];
        return *dynamic_cast<PushChannel<MsgT, DstObjT>*>(channel);
    }

    // Create PushCombinedChannel
    template <typename MsgT, typename CombineT, typename DstObjT>
    static PushCombinedChannel<MsgT, DstObjT, CombineT>& create_push_combined_channel(ChannelSource& src_list,
                                                                                      ObjList<DstObjT>& dst_list,
                                                                                      const std::string& name = "") {
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(default_channel_id++) : name;
        ASSERT_MSG(channel_map.find(channel_name) == channel_map.end(),
                   "ChannelFactoryBase::create_channel: Channel name already exists");
        auto* push_combined_channel = new PushCombinedChannel<MsgT, DstObjT, CombineT>(&src_list, &dst_list);
        channel_map.insert({channel_name, push_combined_channel});
        return *push_combined_channel;
    }

    template <typename MsgT, typename CombineT, typename DstObjT>
    static PushCombinedChannel<MsgT, DstObjT, CombineT>& get_push_combined_channel(const std::string& name = "") {
        ASSERT_MSG(channel_map.find(name) != channel_map.end(),
                   "ChannelFactoryBase::get_channel: Channel name doesn't exist");
        auto* channel = channel_map[name];
        return *dynamic_cast<PushCombinedChannel<MsgT, DstObjT, CombineT>*>(channel);
    }

    // Create MigrateChannel
    template <typename ObjT>
    static MigrateChannel<ObjT>& create_migrate_channel(ObjList<ObjT>& src_list, ObjList<ObjT>& dst_list,
                                                        const std::string& name = "") {
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(default_channel_id++) : name;
        ASSERT_MSG(channel_map.find(channel_name) == channel_map.end(),
                   "ChannelFactoryBase::create_channel: Channel name already exists");
        auto* migrate_channel = new MigrateChannel<ObjT>(&src_list, &dst_list);
        channel_map.insert({channel_name, migrate_channel});
        return *migrate_channel;
    }

    template <typename ObjT>
    static MigrateChannel<ObjT>& get_migrate_channel(const std::string& name = "") {
        ASSERT_MSG(channel_map.find(name) != channel_map.end(),
                   "ChannelFactoryBase::get_channel: Channel name doesn't exist");
        auto* channel = channel_map[name];
        return *dynamic_cast<MigrateChannel<ObjT>*>(channel);
    }

    // Create BroadcastChannel
    template <typename KeyT, typename MsgT>
    static BroadcastChannel<KeyT, MsgT>& create_broadcast_channel(ChannelSource& src_list,
                                                                  const std::string& name = "") {
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(default_channel_id++) : name;
        ASSERT_MSG(channel_map.find(channel_name) == channel_map.end(),
                   "ChannelFactoryBase::create_channel: Channel name already exists");
        auto* broadcast_channel = new BroadcastChannel<KeyT, MsgT>(&src_list);
        channel_map.insert({channel_name, broadcast_channel});
        return *broadcast_channel;
    }

    // Create AsyncPushChannel
    template <typename MsgT, typename ObjT>
    static AsyncPushChannel<MsgT, ObjT>& create_async_push_channel(ObjList<ObjT>& obj_list,
                                                                   const std::string& name = "") {
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(default_channel_id++) : name;
        ASSERT_MSG(channel_map.find(channel_name) == channel_map.end(),
                   "ChannelFactoryBase::create_channel: Channel name already exists");
        auto* async_push_channel = new AsyncPushChannel<MsgT, ObjT>(&obj_list);
        channel_map.insert({channel_name, async_push_channel});
        return *async_push_channel;
    }

    template <typename MsgT, typename ObjT>
    static AsyncPushChannel<MsgT, ObjT>& get_async_push_channel(const std::string& name = "") {
        ASSERT_MSG(channel_map.find(name) != channel_map.end(),
                   "ChannelFactoryBase::get_channel: Channel name doesn't exist");
        auto* channel = channel_map[name];
        return *dynamic_cast<AsyncPushChannel<MsgT, ObjT>*>(channel);
    }

    // Create AsyncMigrateChannel
    template <typename ObjT>
    static AsyncMigrateChannel<ObjT>& create_async_migrate_channel(ObjList<ObjT>& obj_list,
                                                                   const std::string& name = "") {
        std::string channel_name = name.empty() ? channel_name_prefix + std::to_string(default_channel_id++) : name;
        ASSERT_MSG(channel_map.find(channel_name) == channel_map.end(),
                   "ChannelFactoryBase::create_channel: Channel name already exists");
        auto* async_migrate_channel = new AsyncMigrateChannel<ObjT>(&obj_list);
        channel_map.insert({channel_name, async_migrate_channel});
        return *async_migrate_channel;
    }

    template <typename ObjT>
    static AsyncMigrateChannel<ObjT>& get_async_migrate_channel(const std::string& name = "") {
        ASSERT_MSG(channel_map.find(name) != channel_map.end(),
                   "ChannelFactoryBase::get_channel: Channel name doesn't exist");
        auto* channel = channel_map[name];
        return *dynamic_cast<AsyncMigrateChannel<ObjT>*>(channel);
    }

    template <typename KeyT, typename MsgT>
    static BroadcastChannel<KeyT, MsgT>& get_broadcast_channel(const std::string& name = "") {
        ASSERT_MSG(channel_map.find(name) != channel_map.end(),
                   "ChannelFactoryBase::get_channel: Channel name doesn't exist");
        auto* channel = channel_map[name];
        return *dynamic_cast<BroadcastChannel<KeyT, MsgT>*>(channel);
    }

    static void drop_channel(const std::string& name) {
        ASSERT_MSG(channel_map.find(name) != channel_map.end(),
                   "ChannelFactoryBase::drop_channel: Channel name doesn't exist");
        delete channel_map[name];
        channel_map.erase(name);
    }

    static size_t size() { return channel_map.size(); }

   protected:
    static thread_local std::unordered_map<std::string, ChannelBase*> channel_map;
    static thread_local int default_channel_id;
    static const char* channel_name_prefix;
};

}  // namespace husky
