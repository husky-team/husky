#include "core/channel/broadcast_channel.hpp"

#include <iostream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "base/serialization.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist.hpp"
#include "core/worker_info.hpp"

namespace husky {
namespace {

class TestBroadcastChannel : public testing::Test {
   public:
    TestBroadcastChannel() {}
    ~TestBroadcastChannel() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class Obj {
   public:
    using KeyT = int;
    KeyT key;
    const KeyT& id() const { return key; }
    Obj() {}
    explicit Obj(const KeyT& k) : key(k) {}
};

// Create broadcast without setting
template <typename KeyT, typename ValueT>
BroadcastChannel<KeyT, ValueT> create_broadcast_channel(ChannelSource& src_list) {
    BroadcastChannel<KeyT, ValueT> broadcast_channel(&src_list);
    return broadcast_channel;
}

TEST_F(TestBroadcastChannel, Create) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_process_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;

    // BroadcastChannel
    auto broadcast_channel = create_broadcast_channel<int, int>(src_list);
    broadcast_channel.setup(0, 0, workerinfo, &mailbox);
}

TEST_F(TestBroadcastChannel, Broadcast) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_process_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;

    // BroadcastChannel
    auto broadcast_channel = create_broadcast_channel<int, std::string>(src_list);
    broadcast_channel.setup(0, 0, workerinfo, &mailbox);

    // broadcast
    // Round 1
    broadcast_channel.broadcast(23, "abc");
    broadcast_channel.broadcast(45, "bbb");
    broadcast_channel.flush();

    broadcast_channel.prepare_broadcast();
    EXPECT_EQ(broadcast_channel.get(23), "abc");
    EXPECT_EQ(broadcast_channel.get(45), "bbb");

    // Round 2
    broadcast_channel.broadcast(23, "a");
    broadcast_channel.broadcast(45, "b");
    broadcast_channel.flush();

    broadcast_channel.prepare_broadcast();
    EXPECT_EQ(broadcast_channel.get(23), "a");
    EXPECT_EQ(broadcast_channel.get(45), "b");

    // Round 3
    broadcast_channel.broadcast(23, "c");
    broadcast_channel.broadcast(45, "d");
    broadcast_channel.flush();

    broadcast_channel.prepare_broadcast();
    EXPECT_EQ(broadcast_channel.get(23), "c");
    EXPECT_EQ(broadcast_channel.get(45), "d");
}

TEST_F(TestBroadcastChannel, BroadcastClearDict) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_process_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;

    // BroadcastChannel
    auto broadcast_channel = create_broadcast_channel<int, std::string>(src_list);
    broadcast_channel.setup(0, 0, workerinfo, &mailbox);

    // broadcast
    // Round 1
    broadcast_channel.broadcast(23, "abc");
    broadcast_channel.flush();

    broadcast_channel.prepare_broadcast();
    EXPECT_EQ(broadcast_channel.get(23), "abc");

    // Round 2
    broadcast_channel.flush();
    broadcast_channel.prepare_broadcast();
    EXPECT_EQ(broadcast_channel.get(23), "abc");  // Last round result remain valid

    // set clear dict
    broadcast_channel.set_clear_dict(true);

    // Round 3
    broadcast_channel.flush();
    broadcast_channel.prepare_broadcast();
    EXPECT_EQ(broadcast_channel.find(23), false);  // Last round result is invalid
}

TEST_F(TestBroadcastChannel, MultiThread) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);
    hashring.insert(1, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    // Mailbox_0
    LocalMailbox mailbox_0(&zmq_context);
    mailbox_0.set_thread_id(0);
    el.register_mailbox(mailbox_0);
    // Mailbox_1
    LocalMailbox mailbox_1(&zmq_context);
    mailbox_1.set_thread_id(1);
    el.register_mailbox(mailbox_1);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.add_worker(0, 1, 1);
    workerinfo.set_process_id(0);

    std::thread th1 = std::thread([&]() {
        // ObjList Setup
        ObjList<Obj> src_list;

        // BroacastChannel
        auto broadcast_channel = create_broadcast_channel<int, std::string>(src_list);
        broadcast_channel.setup(0, 0, workerinfo, &mailbox_0);

        // broadcast
        // Round 1
        broadcast_channel.broadcast(23, "abc");
        broadcast_channel.flush();

        broadcast_channel.prepare_broadcast();
        EXPECT_EQ(broadcast_channel.get(23), "abc");
        EXPECT_EQ(broadcast_channel.get(12), "ddd");
    });
    std::thread th2 = std::thread([&]() {
        // ObjList Setup
        ObjList<Obj> src_list;

        // BroacastChannel
        auto broadcast_channel = create_broadcast_channel<int, std::string>(src_list);
        broadcast_channel.setup(1, 1, workerinfo, &mailbox_1);

        // broadcast
        // Round 1
        broadcast_channel.broadcast(12, "ddd");
        broadcast_channel.flush();

        broadcast_channel.prepare_broadcast();
        EXPECT_EQ(broadcast_channel.get(23), "abc");
        EXPECT_EQ(broadcast_channel.get(12), "ddd");
    });

    th1.join();
    th2.join();
}

}  // namespace
}  // namespace husky
