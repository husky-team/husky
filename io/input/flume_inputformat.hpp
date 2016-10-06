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

#pragma once

#include <string>
#include <thread>
#include <vector>

#include "flume_connector/ThriftSourceProtocol.h"
#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/server/TSimpleServer.h"
#include "thrift/transport/TBufferTransports.h"
#include "thrift/transport/TServerSocket.h"

#include "boost/utility/string_ref.hpp"
#include "io/input/inputformat_base.hpp"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

// flume processor
class ThriftSourceProtocolHandler : virtual public ThriftSourceProtocolIf {
   public:
    ThriftSourceProtocolHandler();
    ~ThriftSourceProtocolHandler();
    Status::type append(const ThriftFlumeEvent& event);
    Status::type appendBatch(const std::vector<ThriftFlumeEvent>& events);
    boost::string_ref get_next_data();
    void clear_buffer();

   protected:
    // to do: a circle to save memory
    std::vector<std::string> buffer_;
    pthread_rwlock_t buffer_rwlock_;
    int last_read_pos_ = -1;
};

namespace husky {
namespace io {

class FlumeInputFormat : public InputFormatBase {
   public:
    typedef boost::string_ref RecordT;

    explicit FlumeInputFormat(std::string receiver_host = "localhost", int receiver_port = 2016);
    ~FlumeInputFormat();

    virtual bool next(boost::string_ref& ref);
    virtual bool is_setup() const;
    void start_listen();
    void stop_listen();
    bool is_listening_worker();

   protected:
    void listen_();

    std::string receiver_host_;
    int receiver_port_;
    ThriftSourceProtocolHandler* flume_handler_;
    TSimpleServer* server_;
};

}  // namespace io
}  // namespace husky
