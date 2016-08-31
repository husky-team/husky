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

#include "core/channel/channel_source.hpp"

#include <unordered_map>
#include <vector>

#include "core/channel/channel_base.hpp"

namespace husky {

std::vector<ChannelBase*> ChannelSource::get_outchannels() {
    std::vector<ChannelBase*> ret;
    for (auto& kv : outchannels_) {
        ret.push_back(kv.second);
    }
    return ret;
}

void ChannelSource::register_outchannel(size_t channel_id, ChannelBase* outchannel) {
    outchannels_.insert({channel_id, outchannel});
}

void ChannelSource::deregister_outchannel(size_t channel_id) {
    if (outchannels_.find(channel_id) != outchannels_.end())
        outchannels_.erase(channel_id);
}

}  // namespace husky
