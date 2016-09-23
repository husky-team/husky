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

#include "core/coordinator.hpp"

#include <string>

#include "base/serialization.hpp"
#include "core/context.hpp"
#include "core/worker_info.hpp"
#include "core/zmq_helpers.hpp"

namespace husky {

Coordinator::Coordinator() : proc_id_(-1), zmq_coord_(nullptr) {}

Coordinator::~Coordinator() {
    delete zmq_coord_;
    zmq_coord_ = nullptr;
}

void Coordinator::serve() {
    if (zmq_coord_)
        return;

    proc_id_ = Context::get_worker_info()->get_proc_id();
    std::string hostname = Context::get_param("hostname") + "-" + std::to_string(proc_id_);

    zmq_coord_ = new zmq::socket_t(Context::get_zmq_context(), ZMQ_DEALER);
    zmq_coord_->setsockopt(ZMQ_IDENTITY, hostname.c_str(), hostname.size());
    int linger = 2000;
    zmq_coord_->setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
    zmq_coord_->connect("tcp://" + Context::get_config()->get_master_host() + ":" +
                        std::to_string(Context::get_config()->get_master_port()));
}

base::BinStream Coordinator::ask_master(base::BinStream& question, size_t type) {
    coord_lock_.lock();

    // Question type
    zmq_sendmore_dummy(zmq_coord_);
    zmq_sendmore_int32(zmq_coord_, type);

    // Question body
    zmq_send_binstream(zmq_coord_, question);

    zmq_recv_dummy(zmq_coord_);
    base::BinStream answer = zmq_recv_binstream(zmq_coord_);

    coord_lock_.unlock();

    return answer;
}

void Coordinator::notify_master(base::BinStream& message, size_t type) {
    coord_lock_.lock();

    // type
    zmq_sendmore_dummy(zmq_coord_);
    zmq_sendmore_int32(zmq_coord_, type);

    // Message body
    zmq_send_binstream(zmq_coord_, message);
    coord_lock_.unlock();
}

}  // namespace husky
