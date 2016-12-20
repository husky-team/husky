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
    base::log_init(av[0]);

    Config config;
    WorkerInfo worker_info;

    bool succ = config.init_with_args(ac, av, customized, &worker_info);
    if (succ) {
        if (!config.get_log_dir().empty())
            base::log_to_dir(config.get_log_dir());
        Context::set_config(std::move(config));
        Context::set_worker_info(std::move(worker_info));
    }
    return succ;
}

void run_job(const std::function<void()>& job) {
    // Exception thrown by Context::get_process_id() means that this machine
    // is not chosen to run jobs
    try {
        Context::get_process_id();
    } catch (base::HuskyException& e) {
        return;
    }

    Context::create_mailbox_env();
    // Initialize coordinator
    Context::get_coordinator()->serve();

    base::SessionLocal::initialize();
    // Initialize worker threads
    std::vector<boost::thread*> threads;
    int local_id = 0;
    auto& worker_info = Context::get_worker_info();
    for (int i = 0; i < Context::get_num_workers(); i++) {
        if (worker_info.get_process_id(i) != Context::get_process_id())
            continue;

        threads.push_back(new boost::thread([=]() {
            Context::set_local_tid(local_id);
            Context::set_global_tid(i);

            job();
            base::SessionLocal::thread_finalize();

            base::BinStream finish_signal;
            finish_signal << Context::get_param("hostname") << i;
            Context::get_coordinator()->notify_master(finish_signal, TYPE_EXIT);
        }));
        local_id += 1;
    }

    for (int i = 0; i < threads.size(); i++) {
        threads[i]->join();
        delete threads[i];
    }

    base::SessionLocal::finalize();
}

}  // namespace husky
