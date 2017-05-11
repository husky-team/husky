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

#include "core/channel/channel_store_base.hpp"

#include <string>
#include <unordered_map>

#include "base/session_local.hpp"

namespace husky {

thread_local ChannelIdMap* ChannelStoreBase::s_channel_id_map = nullptr;
thread_local ChannelNameMap* ChannelStoreBase::s_channel_name_map = nullptr;

// set finalize_all_channels priority to Level2, the higher the level, the higher the priority
static thread_local base::RegSessionThreadFinalizer finalize_all_channels(base::SessionLocalPriority::Level2, []() {
    ChannelStoreBase::drop_all_channels();
    ChannelStoreBase::free_channel_map();
});

bool ChannelStoreBase::has_channel(const size_t id) {
    ChannelIdMap& channel_map = get_channel_id_map();
    return channel_map.find(id) != channel_map.end();
}

bool ChannelStoreBase::has_channel(const std::string& channel_name) {
    ChannelNameMap& channel_map = get_channel_name_map();
    return channel_map.find(channel_name) != channel_map.end();
}

void ChannelStoreBase::drop_channel(const size_t id) {
    ChannelIdMap& channel_map = get_channel_id_map();
    if (channel_map.find(id) == channel_map.end())
        throw base::HuskyException("ChannelStoreBase::drop_channel: Channel id doesn't exist");

    delete channel_map[id];
    channel_map.erase(id);
}

void ChannelStoreBase::drop_channel(const std::string& channel_name) {
    ChannelNameMap& channel_map = get_channel_name_map();
    if (channel_map.find(channel_name) == channel_map.end())
        throw base::HuskyException("ChannelStoreBase::drop_channel: Channel name "+channel_name+" doesn't exist");

    drop_channel(channel_map.at(channel_name)->get_channel_id());
    channel_map.erase(channel_name);
}

void ChannelStoreBase::drop_all_channels() {
    if (s_channel_id_map == nullptr)
        return;

    for (auto& channel_pair : (*s_channel_id_map))
        delete channel_pair.second;

    s_channel_id_map->clear();
    s_channel_name_map->clear();
}

void ChannelStoreBase::init_channel_map() {
    if (s_channel_id_map == nullptr)
        s_channel_id_map = new ChannelIdMap();
    if (s_channel_name_map == nullptr)
        s_channel_name_map = new ChannelNameMap();
}

void ChannelStoreBase::free_channel_map() {
    delete s_channel_id_map;
    s_channel_id_map = nullptr;

    delete s_channel_name_map;
    s_channel_name_map = nullptr;
}

ChannelIdMap& ChannelStoreBase::get_channel_map() {
    return get_channel_id_map();
}

ChannelIdMap& ChannelStoreBase::get_channel_id_map() {
    init_channel_map();
    return *s_channel_id_map;
}

ChannelNameMap& ChannelStoreBase::get_channel_name_map() {
    init_channel_map();
    return *s_channel_name_map;
}

}  // namespace husky
