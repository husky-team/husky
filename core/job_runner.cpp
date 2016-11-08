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

#include "core/job_runner.hpp"

#include <string>
#include <thread>
#include <vector>

#include "boost/thread.hpp"

#include "base/log.hpp"
#include "base/serialization.hpp"
#include "base/session_local.hpp"
#include "core/config.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/coordinator.hpp"
#include "core/mailbox.hpp"
#include "core/worker_info.hpp"

namespace husky {

bool init_with_args(int ac, char** av, const std::vector<std::string>& customized) {
    Context::init_global();
    return Context::get_config()->init_with_args(ac, av, customized);
}

void run_job(const std::function<void()>& job) {
    zmq::context_t& zmq_context = Context::get_zmq_context();
    WorkerInfo& worker_info = *(Context::get_worker_info());

    // Initialize mailbox sub-system
    auto* el = new MailboxEventLoop(&zmq_context);
    el->set_process_id(worker_info.get_proc_id());
    std::vector<LocalMailbox*> mailboxes;
    for (int i = 0; i < worker_info.get_num_processes(); i++)
        el->register_peer_recver(
            i, "tcp://" + worker_info.get_host(i) + ":" + std::to_string(Context::get_config()->get_comm_port()));
    for (int i = 0; i < worker_info.get_num_workers(); i++) {
        if (worker_info.get_proc_id(i) != worker_info.get_proc_id()) {
            el->register_peer_thread(worker_info.get_proc_id(i), i);
        } else {
            auto* mailbox = new LocalMailbox(&zmq_context);
            mailbox->set_thread_id(i);
            el->register_mailbox(*mailbox);
            mailboxes.push_back(mailbox);
        }
    }
    auto* recver = new CentralRecver(&zmq_context, Context::get_recver_bind_addr());

    // Initialize coordinator
    Context::get_coordinator().serve();

    base::SessionLocal::initialize();
    // Initialize worker threads
    std::vector<boost::thread*> threads;
    int local_id = 0;
    for (int i = 0; i < worker_info.get_num_workers(); i++) {
        if (worker_info.get_proc_id(i) != worker_info.get_proc_id())
            continue;

        threads.push_back(new boost::thread([=, &zmq_context, &el, &mailboxes]() {
            Context::init_local();
            Context::set_local_tid(local_id);
            Context::set_global_tid(i);
            Context::set_mailbox(mailboxes[local_id]);

            job();
            base::SessionLocal::thread_finalize();
            Context::finalize_local();

            base::BinStream finish_signal;
            finish_signal << Context::get_param("hostname") << i;
            Context::get_coordinator().notify_master(finish_signal, TYPE_EXIT);
        }));
        local_id += 1;
    }

    for (int i = 0; i < threads.size(); i++) {
        threads[i]->join();
        delete threads[i];
    }
    delete recver;
    delete el;
    for (int i = 0; i < threads.size(); i++)
        delete mailboxes[i];

    base::SessionLocal::finalize();
    Context::finalize_global();
}

}  // namespace husky
