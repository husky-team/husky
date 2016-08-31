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

#include "zmq.hpp"

#include "core/config.hpp"
#include "core/coordinator.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/worker_info.hpp"

namespace husky {
namespace Context {

struct Global {
    Config config;
    zmq::context_t* zmq_context_ptr = nullptr;
    WorkerInfo worker_info;
    HashRing hash_ring;
    Coordinator coordinator;
};

struct Local {
    int global_thread_id = -1;
    int local_thread_id = -1;
    LocalMailbox* mailbox = nullptr;
};

// The following are global methods

void init_global();
void finalize_global();

Global* get_global();
std::string get_param(const std::string& key);
Config* get_config();
zmq::context_t& get_zmq_context();
WorkerInfo* get_worker_info();
HashRing* get_hashring();
std::string get_recver_bind_addr();

// The following are local methods

void init_local();
void finalize_local();
void set_local_tid(int local_tid);
void set_global_tid(int global_tid);
void set_mailbox(LocalMailbox* mailbox);

int get_local_tid();
int get_global_tid();
LocalMailbox* get_mailbox();
Coordinator& get_coordinator();

}  // namespace Context
}  // namespace husky
