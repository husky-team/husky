// Copyright 2015 Husky Team
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

#include <unistd.h>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "flume_connector/ThriftSourceProtocol.h"
#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/server/TSimpleServer.h"
#include "thrift/transport/TBufferTransports.h"
#include "thrift/transport/TServerSocket.h"

#include "base/log.hpp"
#include "boost/utility/string_ref.hpp"
#include "core/context.hpp"
#include "io/input/flume_inputformat.hpp"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

ThriftSourceProtocolHandler::ThriftSourceProtocolHandler() { pthread_rwlock_init(&buffer_rwlock_, NULL); }

ThriftSourceProtocolHandler::~ThriftSourceProtocolHandler() { pthread_rwlock_destroy(&buffer_rwlock_); }

Status::type ThriftSourceProtocolHandler::append(const ThriftFlumeEvent& event) {
    pthread_rwlock_wrlock(&buffer_rwlock_);
    buffer_.push_back(event.body);
    pthread_rwlock_unlock(&buffer_rwlock_);
}

Status::type ThriftSourceProtocolHandler::appendBatch(const std::vector<ThriftFlumeEvent>& events) {
    pthread_rwlock_wrlock(&buffer_rwlock_);
    for (int i = 0; i < events.size(); ++i) {
        std::string tmp = events[i].body;
        buffer_.push_back(events[i].body);
    }
    pthread_rwlock_unlock(&buffer_rwlock_);
}

boost::string_ref ThriftSourceProtocolHandler::get_next_data() {
    pthread_rwlock_rdlock(&buffer_rwlock_);
    if (buffer_.size() == 0 || last_read_pos_ == buffer_.size() - 1) {
        pthread_rwlock_unlock(&buffer_rwlock_);
        return "";
    } else {
        last_read_pos_++;
        boost::string_ref ref = buffer_[last_read_pos_];
        pthread_rwlock_unlock(&buffer_rwlock_);
        return ref;
    }
}

void ThriftSourceProtocolHandler::clear_buffer() { buffer_.clear(); }

namespace husky {
namespace io {

enum FlumeInputFormatSetUp {
    NotSetUp = 0,
    InputSetUp = 1 << 1,
    AllSetUp = InputSetUp,
};

FlumeInputFormat::FlumeInputFormat(std::string rcv_host, int rcv_port) {
    // //debug
    // std::string log = "rcv_host ";
    // log += rcv_host;
    // log += " and port ";
    // log += std::to_string(rcv_port);
    // base::log_msg(log);
    // //debug
    receiver_host_ = rcv_host;
    receiver_port_ = rcv_port;
    if (!is_listening_worker()) {
        is_setup_ = FlumeInputFormatSetUp::AllSetUp;
        flume_handler_ = NULL;
        server_ = NULL;
        return;
    }

    // else
    flume_handler_ = new ThriftSourceProtocolHandler();

    shared_ptr<ThriftSourceProtocolHandler> handler(flume_handler_);
    shared_ptr<TProcessor> processor(new ThriftSourceProtocolProcessor(handler));

    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());

    shared_ptr<TServerTransport> serverTransport(new TServerSocket(receiver_port_));
    shared_ptr<TProtocolFactory> protocolFactory(new TCompactProtocolFactory());

    server_ = new TSimpleServer(processor, serverTransport, transportFactory, protocolFactory);

    // start_listen();
}

FlumeInputFormat::~FlumeInputFormat() { stop_listen(); }

bool FlumeInputFormat::is_setup() const { return !(is_setup_ ^ FlumeInputFormatSetUp::AllSetUp); }

void FlumeInputFormat::start_listen() {
    is_setup_ = FlumeInputFormatSetUp::AllSetUp;
    if (!is_listening_worker())
        return;

    // for worker 0 on listening machines, spawn a new thread to listen flume data
    std::thread lst_thread(&FlumeInputFormat::listen_, this);
    lst_thread.detach();

    char hostname[1024];
    gethostname(hostname, 1023);
    std::string log = "worker ";
    log += std::to_string(husky::Context::get_global_tid());
    log += " in host ";
    log += hostname;
    log += " is listening on port " + std::to_string(receiver_port_);
    husky::base::log_msg(log);
}

void FlumeInputFormat::listen_() { server_->serve(); }

void FlumeInputFormat::stop_listen() {
    if (!is_listening_worker())
        return;
    if (is_listening_worker())
        server_->stop();
}

bool FlumeInputFormat::is_listening_worker() {
    // If only one machine listens, return false for other machines
    if (receiver_host_ != "localhost") {
        char hostname[1024];
        gethostname(hostname, 1023);
        if (strcmp(hostname, receiver_host_.c_str()) != 0)
            return false;
    }

    // if it is the listening machine but not the listening worker
    auto local_id = husky::Context::get_local_tid();
    if (local_id != 0)
        return false;
    else
        return true;
}

bool FlumeInputFormat::next(boost::string_ref& ref) {
    // In the same machine, workers except first one do nothing
    if (!is_listening_worker()) {
        return false;
    } else {
        ref = flume_handler_->get_next_data();

        if (ref.size() == 0)
            return false;
        else
            return true;
    }
}

}  // namespace io
}  // namespace husky
