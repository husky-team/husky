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

#include <atomic>
#include <string>
#include <vector>

#include "base/serialization.hpp"
#include "base/thread_support.hpp"
#include "core/zmq_helpers.hpp"

namespace husky {

template <typename CollectT>
class Shuffler {
   public:
    Shuffler() {}

    Shuffler(Shuffler&& s) : collection_(s.collection_) { s.collection_ = nullptr; }

    CollectT& storage() {
        if (collection_ == nullptr)
            collection_ = new CollectT();
        return *collection_;
    }

    virtual ~Shuffler() {
        if (collection_ != nullptr)
            delete collection_;
        collection_ = nullptr;
    }

   protected:
    CollectT* collection_ = nullptr;
};

template <typename CellT>
class ShuffleCombiner {
   protected:
    typedef std::vector<CellT> CollectT;

   public:
    ShuffleCombiner() {}

    ShuffleCombiner(ShuffleCombiner&& sc) {
        messages_ = std::move(sc.messages_);
        publisher_ = sc.publisher_;
        subscriber_ = sc.subscriber_;
        num_units_ = sc.num_units_;
        num_local_workers_ = sc.num_local_workers_;
        channel_id_ = sc.channel_id_;
        local_id_ = sc.local_id_;
        sc.num_units_ = 0;
        sc.num_local_workers_ = 0;
        sc.channel_id_ = -1;
        sc.local_id_ = -1;
        sc.publisher_ = nullptr;
        sc.subscriber_ = nullptr;
    }

    virtual ~ShuffleCombiner() {
        messages_.clear();
        delete publisher_;
        delete subscriber_;
    }

    void init(int num_global_threads, int num_local_threads, int channel_id, int local_id) {
        num_units_ = num_global_threads;
        num_local_workers_ = num_local_threads;
        channel_id_ = channel_id;
        local_id_ = local_id;
        messages_.resize(num_units_);
        publisher_ = channel_sc_socket_map_[channel_id][local_id_];
        subscriber_ = channel_sc_socket_map_[channel_id][local_id_ + num_local_workers_];
    }

    void send_shuffler_buffer() { husky::zmq_send_int32(publisher_, local_id_, ZMQ_NOBLOCK); }

    int access_next() {
        int worker_id = husky::zmq_recv_int32(subscriber_);
        return worker_id;
    }

    CollectT& storage(unsigned idx) { return messages_[idx].storage(); }

    static void init_sockets(int num_local_workers, int channel_id, zmq::context_t& context) {
        if (channel_sc_socket_map_.find(channel_id) == channel_sc_socket_map_.end()) {
            // TODO(louis): all channels share the same set of sockets
            std::vector<zmq::socket_t*> socket_list(2 * num_local_workers);
            // publisher
            std::string endpoint_prefix =
                "inproc://shuffle_combiner_channel_" + std::to_string(channel_id) + "_worker_";
            for (int i = 0; i < num_local_workers; i++) {
                std::string endpoint = endpoint_prefix + std::to_string(i);
                zmq::socket_t* pub = new zmq::socket_t(context, ZMQ_PUB);
                ASSERT_MSG(pub != nullptr, "init_sockets: nullptr");
                pub->bind(endpoint);
                socket_list[i] = pub;
            }
            // subscriber subscribe to every other local workers except itself
            for (int i = 0; i < num_local_workers; i++) {
                zmq::socket_t* sub = new zmq::socket_t(context, ZMQ_SUB);
                std::string subscribe_list = "";
                for (int j = 0; j < num_local_workers; j++) {
                    if (i == j)
                        continue;
                    std::string endpoint = endpoint_prefix + std::to_string(j);
                    subscribe_list += endpoint + " ";
                    sub->connect(endpoint);
                    sub->setsockopt(ZMQ_SUBSCRIBE, "", 0);
                }
                socket_list[num_local_workers + i] = sub;
            }
            channel_sc_socket_map_[channel_id] = std::move(socket_list);
        }
    }

    static void erase_key(int channel_id) { channel_sc_socket_map_.erase(channel_id); }

   protected:
    static std::unordered_map<int, std::vector<zmq::socket_t*>> channel_sc_socket_map_;
    std::vector<Shuffler<CollectT>> messages_;
    int num_units_ = 0;
    int num_local_workers_ = 0;
    int channel_id_ = -1;
    int local_id_ = -1;
    zmq::socket_t* publisher_ = nullptr;
    zmq::socket_t* subscriber_ = nullptr;
};

template <typename CellT>
std::unordered_map<int, std::vector<zmq::socket_t*>> ShuffleCombiner<CellT>::channel_sc_socket_map_;
}  // namespace husky
