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
#include <vector>

#include "core/config.hpp"
#include "core/coordinator.hpp"
#include "core/mailbox.hpp"
#include "core/worker_info.hpp"

namespace husky {

struct ContextGlobal {
    ~ContextGlobal() {
        // Order is important
        local_mailboxes_.clear();
        central_recver.reset(nullptr);
        mailbox_event_loop.reset(nullptr);
    }

    zmq::context_t zmq_context_;
    std::vector<std::unique_ptr<LocalMailbox>> local_mailboxes_;
    std::unique_ptr<MailboxEventLoop> mailbox_event_loop;
    std::unique_ptr<CentralRecver> central_recver;
    Config config;
    Coordinator coordinator;
    WorkerInfo worker_info;
};

struct ContextLocal {
    int global_worker_thread_id = -1;
    int local_worker_thread_id = -1;
};

class Context {
   public:
    // The following are global methods

    /// \brief Initialize the mailbox sub-system
    ///
    /// This method can only be called after a Husky configuration is loaded.
    /// This will initialize the local mailbox for each thread, as well as setting up the mailbox
    /// event-loop and the mailbox central receiver.
    static void create_mailbox_env();

    static LocalMailbox* get_mailbox(int local_tid) { return global_.local_mailboxes_.at(local_tid).get(); }

    static std::string get_recver_bind_addr() { return "tcp://*:" + std::to_string(global_.config.get_comm_port()); }

    static std::string get_param(const std::string& key) { return global_.config.get_param(key); }

    static std::string get_log_dir() { return global_.config.get_log_dir(); }

    static MailboxEventLoop* get_mailbox_event_loop() { return global_.mailbox_event_loop.get(); }

    static const HashRing& get_hash_ring() { return global_.worker_info.get_hash_ring(); }

    static const WorkerInfo& get_worker_info() { return global_.worker_info; }

    static int get_num_local_workers() {
        return global_.worker_info.get_num_local_workers(global_.worker_info.get_process_id());
    }

    static int get_num_global_workers() { return global_.worker_info.get_num_workers(); }

    static int get_num_workers() { return get_num_global_workers(); }

    static const Config& get_config() { return global_.config; }

    static int get_num_processes() { return global_.worker_info.get_num_processes(); }

    static Coordinator* get_coordinator() { return &global_.coordinator; }

    static int get_process_id() { return global_.worker_info.get_process_id(); }

    static const void set_config(Config&& config) { global_.config = config; }

    static const void set_worker_info(WorkerInfo&& worker_info) { global_.worker_info = worker_info; }

    static zmq::context_t* get_zmq_context() { return &global_.zmq_context_; }

    // The following are local methods

    static int get_global_tid() { return local_.global_worker_thread_id; }

    static int get_local_tid() { return local_.local_worker_thread_id; }

    static void set_global_tid(int global_tid) { local_.global_worker_thread_id = global_tid; }

    static void set_local_tid(int local_tid) { local_.local_worker_thread_id = local_tid; }

    static LocalMailbox* get_mailbox() { return get_mailbox(local_.local_worker_thread_id); }

   protected:
    static ContextGlobal global_;
    static thread_local ContextLocal local_;
};

}  // namespace husky
