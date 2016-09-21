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

#include <sys/stat.h>

#include <fstream>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "zmq.hpp"

#include "base/hash.hpp"
#include "base/log.hpp"
#include "base/serialization.hpp"
#include "core/config.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/hash_ring.hpp"
#include "core/worker_info.hpp"
#include "core/zmq_helpers.hpp"

namespace husky {

using base::BinStream;

Master::Master() {
}

void Master::setup() {
    init_socket();

    finished_workers.clear();
    is_serve = Context::get_param("serve").empty() ? true : std::stoi(Context::get_param("serve"));
    expected_num_workers = Context::get_worker_info()->get_num_workers();

    num_machines = Context::get_config()->get_num_machines();

    for (int i = 0; i < num_workers; i++)
        sync_set.insert(i);

    work_time.resize(expected_num_workers, 0);
    progress.resize(expected_num_workers, -1);
    ready_for_lb = 0;
    lb_step = -2;
    partition_to_donate.resize(partition_table.size(), false);
    debug_num_broadcast = 0;

    for (auto setup_handler : external_setup_handlers) {
        setup_handler();
    }
}

Master::~Master() { delete resp_socket; }

void Master::init_socket() {
    resp_socket = new zmq::socket_t(zmq_context, ZMQ_ROUTER);
    resp_socket->bind("tcp://*:" + std::to_string(Context::get_config()->get_master_port()));
    base::log_msg("Binded to tcp://*:" + std::to_string(Context::get_config()->get_master_port()));
}

void Master::serve() {
    while (true) {
        cur_client = zmq_recv_string(resp_socket);
        zmq_recv_dummy(resp_socket);
        handle_message(zmq_recv_int32(resp_socket), cur_client);
        if (!is_serve && finished_workers.size() == expected_num_workers) {
            base::log_msg("\033[1;32mMASTER FINISHED\033[0m");
            break;
        }
    }
}

void Master::handle_message(uint32_t message, const std::string& id) {
    switch (message) {
    case TYPE_STOP_ASYNC_REQ: {
        // TODO(all): change to num_async and decrease it

        resp_socket->recv(&msg);
        pending_sync_ids.push_back(cur_client);

        base::log_msg("\033[1;34mSTOPPING ASYNC (" + std::to_string(pending_sync_ids.size()) + "/" +
                      std::to_string(num_machines) + ")...\033[0m");

        if (pending_sync_ids.size() == num_machines) {
            for (auto& id : pending_sync_ids) {
                zmq_sendmore_string(resp_socket, id);
                zmq_sendmore_dummy(resp_socket);
                zmq_send_int64(resp_socket, TYPE_STOP_ASYNC_YES);
            }
            pending_sync_ids.clear();
            base::log_msg("\033[1;32mSTOPPED ASYNC\033[0m");
        }
        break;
    }
    case TYPE_START_ASYNC_REQ: {
        resp_socket->recv(&msg);
        pending_async_ids.push_back(id);

        base::log_msg("\033[1;34mSTARTING ASYNC (" + std::to_string(pending_async_ids.size()) + "/" +
                      std::to_string(num_machines) + ")...\033[0m");

        if (pending_async_ids.size() == num_machines) {
            for (auto& id : pending_async_ids) {
                zmq_sendmore_string(resp_socket, id);
                zmq_sendmore_dummy(resp_socket);
                zmq_send_int64(resp_socket, TYPE_START_ASYNC_YES);
            }
            pending_async_ids.clear();
            base::log_msg("\033[1;32mSTARTED ASYNC\033[0m");
        }

        break;
    }
    case TYPE_EXIT: {
        BinStream stream = zmq_recv_binstream(resp_socket);
        std::string worker_name;
        int worker_id;
        stream >> worker_name >> worker_id;
        hash_ring.remove(worker_id);
        num_workers = hash_ring.get_num_workers();
        finished_workers.insert(worker_id);
        base::log_msg("master => worker finsished @" + worker_name + "-" + std::to_string(worker_id));

        BinStream dummy;
        zmq_sendmore_string(resp_socket, cur_client);
        zmq_sendmore_dummy(resp_socket);
        zmq_send_binstream(resp_socket, dummy);
        break;
    }
    case TYPE_JOIN: {
        int worker_id = zmq_recv_int32(resp_socket);
        hash_ring.insert(worker_id);
        num_workers = hash_ring.get_num_workers();

        if (num_workers == expected_num_workers) {
            for (auto id : pending_hash_ring_requester_id) {
                BinStream bin_ring;
                bin_ring << hash_ring;
                zmq_sendmore_string(resp_socket, id);
                zmq_sendmore_dummy(resp_socket);
                zmq_send_binstream(resp_socket, bin_ring);
            }
        }
        break;
    }
    case TYPE_GET_HASH_RING: {
        if (num_workers == expected_num_workers) {
            BinStream bin_ring;
            bin_ring << hash_ring;
            base::log_msg("bin_ring size: " + std::to_string(bin_ring.size()));
            zmq_sendmore_string(resp_socket, cur_client);
            zmq_sendmore_dummy(resp_socket);
            zmq_send_binstream(resp_socket, bin_ring);
        } else {
            pending_hash_ring_requester_id.push_back(cur_client);
        }
        break;
    }
    default: {
        if (external_main_handlers.find(message) != external_main_handlers.end()) {
            external_main_handlers[message]();
        } else {
            throw std::runtime_error("Weird message received");
        }
    }
    }
}

}  // namespace husky

/// Main function
int main(int argc, char** argv) {
    srand(0);
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
        base::log_msg("\033[1;32mMASTER READY\033[0m");
        master.serve();
        return 0;
    }
    return 1;
}
