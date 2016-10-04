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

#include <utility>
#include <vector>

#include "core/channel/channel_base.hpp"
#include "core/mailbox.hpp"
#include "core/utils.hpp"

namespace husky {

/// ChannelManager is used to manage all kinds of channels_ created by users.
/// 1. Distribute binstream received from Mailbox
/// 2. Invoke flush function for every channel used in this list_execute
class ChannelManager {
   public:
    explicit ChannelManager(const std::vector<ChannelBase*>& channels) : channels_(channels) {
        if (channels_.size() > 0)
            mailbox_ = channels_[0]->get_mailbox();
    }

    void poll_and_distribute() {
        if (channels_.size() == 0)
            return;

        std::vector<ChannelBase*> selected_channels;
        std::vector<std::pair<int, int>> channel_progress_pairs;
        for (auto* channel : channels_) {
            // Only consider the channels_ which are flushed and do preparation
            if (channel->is_flushed()) {
                channel->prepare();
                selected_channels.push_back(channel);
                channel_progress_pairs.push_back({channel->get_channel_id(), channel->get_progress()});
            }
        }
        // return if no channel is flushed
        if (channel_progress_pairs.empty())
            return;

        // receive from mailbox_ and distbribute
        int idx = -1;
        std::pair<int, int> pair;
        if (channel_progress_pairs.empty()) {
            return;
        }
        while (mailbox_->poll(channel_progress_pairs, &idx)) {
            ASSERT_MSG(idx != -1, "ChannelManager: Mailbox poll error");
            auto bin = mailbox_->recv(channel_progress_pairs[idx].first, channel_progress_pairs[idx].second);
            selected_channels[idx]->in(bin);
        }

        // reset the flushed_ buffer
        for (auto* ch : selected_channels) {
            ch->reset_flushed();
        }
    }

    void flush() {
        for (auto* channel : channels_) {
            channel->out();
        }
    }

   private:
    std::vector<ChannelBase*> channels_;
    LocalMailbox* mailbox_;
};

}  // namespace husky
