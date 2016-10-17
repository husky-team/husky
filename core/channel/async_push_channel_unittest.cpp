#include "core/channel/async_push_channel.hpp"

#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "base/log.hpp"
#include "base/serialization.hpp"
#include "core/channel/migrate_channel.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist.hpp"
#include "core/worker_info.hpp"

namespace husky {
namespace {

class TestAsyncPushChannel : public testing::Test {
   public:
    TestAsyncPushChannel() {}
    ~TestAsyncPushChannel() {}

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

// Create AsyncPushChannel without setting
template <typename MsgT, typename DstObjT>
AsyncPushChannel<MsgT, DstObjT> create_async_push_channel(ObjList<DstObjT>& obj_list) {
    AsyncPushChannel<MsgT, DstObjT> async_push_channel(&obj_list);
    return async_push_channel;
}
// Create MigrateChannel without setting
template <typename ObjT>
MigrateChannel<ObjT> create_migrate_channel(ObjList<ObjT>& src_list, ObjList<ObjT>& dst_list) {
    MigrateChannel<ObjT> migrate_channel(&src_list, &dst_list);
    return migrate_channel;
}

TEST_F(TestAsyncPushChannel, Create) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0);

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
    workerinfo.add_proc(0, "worker1");
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_num_processes(1);
    workerinfo.set_num_workers(1);
    workerinfo.set_proc_id(0);

    // ObjList Setup
    ObjList<Obj> obj_list;

    // PushChannel
    auto async_push_channel = create_async_push_channel<int>(obj_list);
    async_push_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
}

TEST_F(TestAsyncPushChannel, PushSingle) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0);

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
    workerinfo.add_proc(0, "worker1");
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_num_processes(1);
    workerinfo.set_num_workers(1);
    workerinfo.set_proc_id(0);

    // ObjList Setup
    ObjList<Obj> obj_list;

    // PushChannel
    auto async_push_channel = create_async_push_channel<int>(obj_list);
    async_push_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // push
    async_push_channel.push(123, 10);  // send 123 to 10
    async_push_channel.out();
    // get
    async_push_channel.prepare_messages_test();
    Obj& obj = obj_list.get_data()[0];

    EXPECT_EQ(obj.id(), 10);
    auto msgs = async_push_channel.get(obj);
    EXPECT_EQ(msgs.size(), 1);
    EXPECT_EQ(msgs[0], 123);
}

TEST_F(TestAsyncPushChannel, PushMultipleTime) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // ObjList Setup
    ObjList<Obj> obj_list;

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_proc(0, "worker1");
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_num_processes(1);
    workerinfo.set_num_workers(1);
    workerinfo.set_proc_id(0);

    // PushChannel
    auto async_push_channel = create_async_push_channel<int>(obj_list);
    async_push_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // push to two dst
    async_push_channel.push(123, 10);  // send 123 to 10
    async_push_channel.push(32, 3);
    async_push_channel.push(4, 10);
    async_push_channel.push(532, 3);
    async_push_channel.push(56, 10);
    async_push_channel.out();
    // get
    while (1) {
        if (mailbox.poll_non_block(async_push_channel.get_channel_id(), async_push_channel.get_progress())) {
            auto bin = mailbox.recv(async_push_channel.get_channel_id(), async_push_channel.get_progress());
            async_push_channel.in(bin);
        }
        int count = 0;
        for (auto& obj : obj_list.get_data())
            for (auto& msg : async_push_channel.get(obj))
                count += 1;
        if (count == 5)  // Total 5 msgs
            break;
    }
    EXPECT_EQ(obj_list.get_size(), 2);
}

TEST_F(TestAsyncPushChannel, IncProgress) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0);

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
    workerinfo.add_proc(0, "worker1");
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_num_processes(1);
    workerinfo.set_num_workers(1);
    workerinfo.set_proc_id(0);

    // ObjList Setup
    ObjList<Obj> obj_list;

    // PushChannel
    // Round 1
    auto async_push_channel = create_async_push_channel<int>(obj_list);
    async_push_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // push
    async_push_channel.push(123, 10);  // send 123 to 10
    async_push_channel.out();
    // get
    async_push_channel.prepare_messages_test();
    Obj& obj = obj_list.get_data()[0];

    EXPECT_EQ(obj.id(), 10);
    auto msgs = async_push_channel.get(obj);
    EXPECT_EQ(msgs.size(), 1);
    EXPECT_EQ(msgs[0], 123);

    // Round 2
    async_push_channel.push(456, 10);  // send 123 to 10
    async_push_channel.out();

    async_push_channel.prepare_messages_test();

    EXPECT_EQ(obj.id(), 10);
    msgs = async_push_channel.get(obj);
    EXPECT_EQ(msgs.size(), 1);
    EXPECT_EQ(msgs[0], 456);
}

TEST_F(TestAsyncPushChannel, MultiThread) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0);
    hashring.insert(1);

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
    workerinfo.add_proc(0, "worker1");
    workerinfo.add_worker(0, 0, 0);
    workerinfo.add_worker(0, 1, 1);
    workerinfo.set_num_processes(1);
    workerinfo.set_num_workers(2);
    workerinfo.set_proc_id(0);

    std::thread th1 = std::thread([&]() {
        ObjList<Obj> obj_list;

        obj_list.add_object(Obj(100));
        obj_list.add_object(Obj(18));
        obj_list.add_object(Obj(57));

        // GLobalize
        auto migrate_channel = create_migrate_channel(obj_list, obj_list);
        migrate_channel.setup(0, 0, &workerinfo, &mailbox_0, &hashring);
        for (auto& obj : obj_list.get_data()) {
            int dst_thread_id = hashring.hash_lookup(obj.id());
            if (dst_thread_id != 0) {
                migrate_channel.migrate(obj, dst_thread_id);
            }
        }
        obj_list.deletion_finalize();
        migrate_channel.out();
        migrate_channel.prepare_immigrants();
        obj_list.sort();

        // Push
        auto async_push_channel = create_async_push_channel<int>(obj_list);
        async_push_channel.setup(0, 0, &workerinfo, &mailbox_0, &hashring);
        async_push_channel.push(123, 1);
        async_push_channel.push(123, 1342148);
        async_push_channel.push(123, 5);
        async_push_channel.push(123, 100);
        async_push_channel.push(123, 18);
        async_push_channel.push(123, 57);

        async_push_channel.out();
        async_push_channel.prepare_messages_test();

        for (auto& obj : obj_list.get_data()) {
            auto& msgs = async_push_channel.get(obj);
            EXPECT_EQ(msgs.size(), 2);
        }
    });
    std::thread th2 = std::thread([&]() {
        ObjList<Obj> obj_list;

        obj_list.add_object(Obj(1));
        obj_list.add_object(Obj(1342148));
        obj_list.add_object(Obj(5));

        // GLobalize
        auto migrate_channel = create_migrate_channel(obj_list, obj_list);
        migrate_channel.setup(1, 1, &workerinfo, &mailbox_1, &hashring);
        for (auto& obj : obj_list.get_data()) {
            int dst_thread_id = hashring.hash_lookup(obj.id());
            if (dst_thread_id != 1) {
                migrate_channel.migrate(obj, dst_thread_id);
            }
        }
        obj_list.deletion_finalize();
        migrate_channel.out();
        migrate_channel.prepare_immigrants();
        obj_list.sort();

        // Push
        auto async_push_channel = create_async_push_channel<int>(obj_list);
        async_push_channel.setup(1, 1, &workerinfo, &mailbox_1, &hashring);
        async_push_channel.push(123, 1);
        async_push_channel.push(123, 1342148);
        async_push_channel.push(123, 5);
        async_push_channel.push(123, 100);
        async_push_channel.push(123, 18);
        async_push_channel.push(123, 57);

        async_push_channel.out();
        async_push_channel.prepare_messages_test();

        for (auto& obj : obj_list.get_data()) {
            auto& msgs = async_push_channel.get(obj);
            EXPECT_EQ(msgs.size(), 2);
        }
    });

    th1.join();
    th2.join();
}

}  // namespace
}  // namespace husky
