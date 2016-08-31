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

#include "zmq.hpp"

#include "base/serialization.hpp"
#include "core/utils.hpp"

namespace husky {

using base::BinStream;

// Define blocking constant since it does not exist in zmq.hpp.
// ZMQ_NOBLOCK = ZMQ_DONTWAIT = 1 -> ZMQ_BLOCKING = !ZMQ_NOBLOCK
const int ZMQ_BLOCKING = !ZMQ_NOBLOCK;

// ZMQ send part.

inline void zmq_send_common(zmq::socket_t* socket, const void* data, const size_t& len, int flag = ZMQ_BLOCKING) {
    ASSERT_MSG(socket != nullptr, "zmq::socket_t cannot be nullptr!");
    ASSERT_MSG(data != nullptr || len == 0, "data and len are not matched!");
    while (true)
        try {
            size_t bytes = socket->send(data, len, flag);
            ASSERT_MSG(bytes == len, "zmq::send error!");
            break;
        } catch (zmq::error_t e) {
            switch (e.num()) {
            case EHOSTUNREACH:
            case EINTR:
                continue;
            default:
                throw;
            }
        }
}

inline void zmq_send_dummy(zmq::socket_t* socket, int flag = ZMQ_BLOCKING) {
    zmq_send_common(socket, nullptr, 0, flag);
}

inline void zmq_sendmore_dummy(zmq::socket_t* socket) { zmq_send_dummy(socket, ZMQ_SNDMORE); }

inline void zmq_send_int32(zmq::socket_t* socket, int32_t data, int flag = ZMQ_BLOCKING) {
    zmq_send_common(socket, &data, sizeof(int32_t), flag);
}

inline void zmq_sendmore_int32(zmq::socket_t* socket, int32_t data) { zmq_send_int32(socket, data, ZMQ_SNDMORE); }

inline void zmq_send_int64(zmq::socket_t* socket, int64_t data, int flag = ZMQ_BLOCKING) {
    zmq_send_common(socket, &data, sizeof(int64_t), flag);
}

inline void zmq_sendmore_int64(zmq::socket_t* socket, int64_t data) { zmq_send_int64(socket, data, ZMQ_SNDMORE); }

inline void zmq_send_string(zmq::socket_t* socket, const std::string& data, int flag = ZMQ_BLOCKING) {
    zmq_send_common(socket, data.data(), data.length(), flag);
}

inline void zmq_sendmore_string(zmq::socket_t* socket, const std::string& data) {
    zmq_send_string(socket, data, ZMQ_SNDMORE);
}

// FIXME(legend): Whether it needs BinStream.get_buffer()?
inline void zmq_send_binstream(zmq::socket_t* socket, const BinStream& stream, int flag = ZMQ_BLOCKING) {
    zmq_send_common(socket, stream.get_remained_buffer(), stream.size(), flag);
}

// ZMQ receive part.

inline void zmq_recv_common(zmq::socket_t* socket, zmq::message_t* msg, int flag = ZMQ_BLOCKING) {
    ASSERT_MSG(socket != nullptr, "zmq::socket_t cannot be nullptr!");
    ASSERT_MSG(msg != nullptr, "zmq::message_t cannot be nullptr!");
    while (true)
        try {
            bool successful = socket->recv(msg, flag);
            ASSERT_MSG(successful, "zmq::recv error!");
            break;
        } catch (zmq::error_t e) {
            if (e.num() == EINTR)
                continue;
            throw;
        }
}

inline void zmq_recv_dummy(zmq::socket_t* socket, int flag = ZMQ_BLOCKING) {
    zmq::message_t msg;
    zmq_recv_common(socket, &msg, flag);
}

inline int32_t zmq_recv_int32(zmq::socket_t* socket) {
    zmq::message_t msg;
    zmq_recv_common(socket, &msg);
    return *reinterpret_cast<int32_t*>(msg.data());
}

inline int64_t zmq_recv_int64(zmq::socket_t* socket) {
    zmq::message_t msg;
    zmq_recv_common(socket, &msg);
    return *reinterpret_cast<int64_t*>(msg.data());
}

inline std::string zmq_recv_string(zmq::socket_t* socket) {
    zmq::message_t msg;
    zmq_recv_common(socket, &msg);
    return std::string(reinterpret_cast<char*>(msg.data()), msg.size());
}

inline BinStream zmq_recv_binstream(zmq::socket_t* socket, int flag = ZMQ_BLOCKING) {
    zmq::message_t msg;
    zmq_recv_common(socket, &msg, flag);
    BinStream stream;
    stream.push_back_bytes(reinterpret_cast<char*>(msg.data()), msg.size());
    return stream;
}

}  // namespace husky
