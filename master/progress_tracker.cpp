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

#include "master/progress_tracker.hpp"

#include <string>

#include "base/log.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"

namespace husky {

static ProgressTracker progress_tracker;

ProgressTracker::ProgressTracker() {
    finished_workers_.clear();
    Master::get_instance().register_main_handler(TYPE_EXIT, std::bind(&ProgressTracker::finish_handler, this));
}

void ProgressTracker::finish_handler() {
    auto& master = Master::get_instance();
    auto master_handler = master.get_socket();
    std::string worker_name;
    int worker_id;
    BinStream stream = zmq_recv_binstream(master_handler.get());
    stream >> worker_name >> worker_id;
    finished_workers_.insert(worker_id);

    base::log_msg("master => worker finsished @" + worker_name + "-" + std::to_string(worker_id));

    if (!master.is_continuous() && (finished_workers_.size() == Context::get_num_workers())) {
        master.halt();
    }
}

}  // namespace husky
