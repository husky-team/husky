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

#include "master/master.hpp"

#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "zmq.hpp"

#include "base/exception.hpp"
#include "base/log.hpp"
#include "base/serialization.hpp"
#include "core/config.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/worker_info.hpp"
#include "core/zmq_helpers.hpp"

namespace husky {

using base::BinStream;

Master::Master() {}

void Master::setup() {
    running = true;

    init_socket();

    continuous = Context::get_param("serve").empty() ? true : std::stoi(Context::get_param("serve"));

    for (auto setup_handler : external_setup_handlers) {
        setup_handler();
    }
}

void Master::init_socket() {
    master_socket.reset(new zmq::socket_t(zmq_context, ZMQ_ROUTER));
    master_socket->bind("tcp://*:" + std::to_string(Context::get_config()->get_master_port()));
    base::log_msg("Binded to tcp://*:" + std::to_string(Context::get_config()->get_master_port()));
}

void Master::serve() {
    base::log_msg("\033[1;32mMASTER READY\033[0m");
    while (running) {
        cur_client = zmq_recv_string(master_socket.get());
        zmq_recv_dummy(master_socket.get());
        handle_message(zmq_recv_int32(master_socket.get()), cur_client);
    }
    base::log_msg("\033[1;32mMASTER FINISHED\033[0m");
}

void Master::handle_message(uint32_t message, const std::string& id) {
    if (external_main_handlers.find(message) != external_main_handlers.end()) {
        external_main_handlers[message]();
    } else {
        throw base::HuskyException("Unknown message type!");
    }
}

}  // namespace husky

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("serve");
#ifdef WITH_HDFS
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
#endif
    husky::Context::init_global();
    if (husky::Context::get_config()->init_with_args(argc, argv, args)) {
        auto& master = husky::Master::get_instance();
        master.setup();
        master.serve();
        return 0;
    }
    return 1;
}
