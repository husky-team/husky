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

#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "zmq.hpp"

#include "base/hash.hpp"
#include "core/hash_ring.hpp"
#include "core/zmq_helpers.hpp"

namespace husky {

class Master {
   public:
    static Master& get_instance() {
        static Master master;
        return master;
    }

    void setup();
    virtual ~Master();
    void init_socket();
    void serve();
    void handle_message(uint32_t message, const std::string& id);
    inline int get_num_machines() const { return num_machines; }
    inline zmq::socket_t* get_socket() const { return resp_socket; }
    inline const std::string& get_cur_client() const { return cur_client; }

    inline void register_main_handler(uint32_t msg_type, std::function<void()> handler) {
        external_main_handlers[msg_type] = handler;
    }

    inline void register_setup_handler(std::function<void()> handler) {
        external_setup_handlers.push_back(handler);
    }

   protected:
    Master();

    bool is_serve = true;
    std::set<int> finished_workers;
    HashRing hash_ring;
    int cur_seq_num = 0;
    int expected_num_workers;
    int num_machines;
    int num_workers = 0;
    int ready_for_lb;
    int seq_sub_count = 0;
    std::set<int> async_set;
    std::set<int> sync_set;
    std::set<std::string> machines;
    std::set<std::string> machines_aware_of_failure;
    std::string cur_client;
    std::unordered_map<std::string, std::chrono::time_point<std::chrono::steady_clock>> heartbeat_timestamps;
    std::vector<int> partition_table;
    std::vector<std::pair<int, int>> partition_table_patch;
    std::vector<std::string> worker_to_machine;
    zmq::context_t zmq_context;
    zmq::message_t msg;
    zmq::socket_t* resp_socket;

    /// LB specific
    int debug_num_broadcast;
    int lb_step;
    std::vector<bool> partition_to_donate;
    std::vector<double> work_time;
    std::vector<int> progress;

    // Merge these containers?
    std::vector<std::string> pending_async_ids;
    std::vector<std::string> pending_flush_ids;
    std::vector<std::string> pending_hash_ring_requester_id;
    std::vector<std::string> pending_sync_ids;

    // External handlers
    std::unordered_map<uint32_t, std::function<void()>> external_main_handlers;
    std::vector<std::function<void()>> external_setup_handlers;
};

}  // namespace husky
