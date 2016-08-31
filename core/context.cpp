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

#include "core/context.hpp"

#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "boost/filesystem/path.hpp"
#include "zmq.hpp"

#include "core/config.hpp"
#include "core/coordinator.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/utils.hpp"
#include "core/worker_info.hpp"

namespace husky {
namespace Context {

static Global* global = nullptr;
static thread_local Local* local = nullptr;

// The following are global methods

void init_global() {
    global = new Global();
    global->zmq_context_ptr = new zmq::context_t();
}

void finalize_global() {
    // TODO(Fan): Ugly, delay deleting zmq_context_ptr.
    auto p = global->zmq_context_ptr;
    delete global;
    delete p;
    global = nullptr;
}

Global* get_global() { return global; }

std::string get_param(const std::string& key) { return global->config.get_param(key); }

Config* get_config() { return &(global->config); }

zmq::context_t& get_zmq_context() { return *(global->zmq_context_ptr); }

WorkerInfo* get_worker_info() { return &(global->worker_info); }

HashRing* get_hashring() { return &(global->hash_ring); }

std::string get_recver_bind_addr() { return "tcp://*:" + std::to_string(global->config.get_comm_port()); }

// The following are local methods

void init_local() { local = new Local(); }

void finalize_local() {
    delete local;
    local = nullptr;
}

void set_local_tid(int local_tid) { local->local_thread_id = local_tid; }

void set_global_tid(int global_tid) { local->global_thread_id = global_tid; }

void set_mailbox(LocalMailbox* mailbox) { local->mailbox = mailbox; }

int get_local_tid() { return local->local_thread_id; }

int get_global_tid() { return local->global_thread_id; }

LocalMailbox* get_mailbox() { return local->mailbox; }

Coordinator& get_coordinator() { return global->coordinator; }

}  // namespace Context
}  // namespace husky
