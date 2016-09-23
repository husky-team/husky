#include "core/zmq_helpers.hpp"

#include <string>
#include <thread>

#include "zmq.hpp"

#include "gtest/gtest.h"

#include "base/serialization.hpp"

namespace husky {
namespace {

using base::BinStream;

class ZMQ {
   public:
    ~ZMQ() {
        delete client;
        delete server;
    }
    void init() {
        server = new zmq::socket_t(context, ZMQ_REP);
        server->bind("inproc://test");
        client = new zmq::socket_t(context, ZMQ_REQ);
        client->connect("inproc://test");
    }
    zmq::context_t context;
    zmq::socket_t* server;
    zmq::socket_t* client;
};

class TestZMQHelpers : public testing::Test {
   public:
    TestZMQHelpers() {}
    ~TestZMQHelpers() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestZMQHelpers, InitAndDelete) {
    ZMQ* zmq = new ZMQ();
    ASSERT_TRUE(zmq != NULL);
    zmq->init();
    ASSERT_TRUE(zmq->server != nullptr);
    ASSERT_TRUE(zmq->client != nullptr);
    delete zmq;
}

TEST_F(TestZMQHelpers, SendAndReceive) {
    ZMQ zmq;
    zmq.init();

    std::thread* send = new std::thread([&zmq]() {
        int sync = zmq_recv_int32(zmq.server);
        if (sync == 1) {
            zmq_sendmore_int32(zmq.server, 333);
            zmq_sendmore_dummy(zmq.server);
            zmq_sendmore_int64(zmq.server, 666);
            zmq_sendmore_string(zmq.server, "ok");
            BinStream input;
            input << 1 << std::string("a") << true;
            zmq_send_binstream(zmq.server, input);
        }
    });

    std::thread* receive = new std::thread([&zmq]() {
        zmq_send_int32(zmq.client, 1);
        int type = zmq_recv_int32(zmq.client);
        if (type == 333) {
            zmq_recv_dummy(zmq.client);
            EXPECT_EQ(zmq_recv_int64(zmq.client), 666);
            EXPECT_EQ(zmq_recv_string(zmq.client), "ok");
            BinStream output = zmq_recv_binstream(zmq.client);
            int out_int = 0;
            std::string out_str;
            bool out_bool = false;
            output >> out_int >> out_str >> out_bool;
            EXPECT_EQ(out_int, 1);
            EXPECT_EQ(out_str, "a");
            EXPECT_EQ(out_bool, true);
        }
    });

    receive->join();
    send->join();

    delete receive;
    delete send;
}

}  // namespace
}  // namespace husky
