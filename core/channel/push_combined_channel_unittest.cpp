#include "core/channel/push_combined_channel.hpp"

#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "base/log.hpp"
#include "base/serialization.hpp"
#include "core/channel/migrate_channel.hpp"
#include "core/combiner.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/objlist_unittest.hpp"
#include "core/worker_info.hpp"

namespace husky {
namespace {

class TestPushCombinedChannel : public testing::Test {
   public:
    TestPushCombinedChannel() {}
    ~TestPushCombinedChannel() {}

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

// Create PushChannel without setting, for setup
template <typename MsgT, typename CombineT, typename DstObjT>
PushCombinedChannel<MsgT, DstObjT, CombineT> create_push_combined_channel(ChannelSource& src_list,
                                                                          ObjListForTest<DstObjT>& dst_list) {
    PushCombinedChannel<MsgT, DstObjT, CombineT> push_channel(&src_list, &dst_list);
    return push_channel;
}
// Create MigrateChannel without setting
template <typename ObjT>
MigrateChannel<ObjT> create_migrate_channel(ObjListForTest<ObjT>& src_list, ObjList<ObjT>& dst_list) {
    MigrateChannel<ObjT> migrate_channel(&src_list, &dst_list);
    return migrate_channel;
}

TEST_F(TestPushCombinedChannel, Create) {
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
    ObjListForTest<Obj> src_list;
    ObjListForTest<Obj> dst_list;

    // PushChannel
    auto push_channel = create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list);
    push_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
}

TEST_F(TestPushCombinedChannel, PushSingle) {
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
    ObjListForTest<Obj> src_list;
    ObjListForTest<Obj> dst_list;

    // PushChannel
    auto push_channel = create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list);
    push_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // push
    push_channel.push(123, 10);  // send 123 to 10
    push_channel.flush();
    // get
    push_channel.prepare_messages();
    Obj& obj = dst_list.get_data()[0];

    EXPECT_EQ(obj.id(), 10);
    auto msgs = push_channel.get(obj);
    EXPECT_EQ(msgs, 123);
}

TEST_F(TestPushCombinedChannel, PushMultipleTime) {
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
    ObjListForTest<Obj> src_list;
    ObjListForTest<Obj> dst_list;

    // PushChannel
    auto push_channel = create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list);
    push_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // push to two dst
    push_channel.push(123, 10);  // send 123 to 10
    push_channel.push(32, 3);
    push_channel.push(4, 10);
    push_channel.push(532, 3);
    push_channel.push(56, 10);
    push_channel.flush();
    // get
    push_channel.prepare_messages();
    EXPECT_EQ(dst_list.get_size(), 2);
    int count = 0;
    for (auto& obj : dst_list.get_data()) {
        if (obj.id() == 3)
            EXPECT_EQ(push_channel.get(obj), 564);
        if (obj.id() == 10)
            EXPECT_EQ(push_channel.get(obj), 183);
    }
}

TEST_F(TestPushCombinedChannel, IncProgress) {
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
    ObjListForTest<Obj> src_list;
    ObjListForTest<Obj> dst_list;

    // PushChannel
    // Round 1
    auto push_channel = create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list);
    push_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // push
    push_channel.push(123, 10);  // send 123 to 10
    push_channel.flush();
    // get
    push_channel.prepare_messages();
    Obj& obj = dst_list.get_data()[0];

    EXPECT_EQ(obj.id(), 10);
    auto msgs = push_channel.get(obj);
    EXPECT_EQ(msgs, 123);

    // Round 2
    push_channel.push(456, 10);  // send 123 to 10
    push_channel.flush();

    push_channel.prepare_messages();

    EXPECT_EQ(obj.id(), 10);
    msgs = push_channel.get(obj);
    EXPECT_EQ(msgs, 456);
}

TEST_F(TestPushCombinedChannel, MultiThread) {
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
        ObjListForTest<Obj> src_list;

        src_list.add_object(Obj(100));
        src_list.add_object(Obj(18));
        src_list.add_object(Obj(57));

        // GLobalize
        auto migrate_channel = create_migrate_channel(src_list, src_list);
        migrate_channel.setup(0, 0, &workerinfo, &mailbox_0, &hashring);
        for (auto& obj : src_list.get_data()) {
            int dst_thread_id = hashring.hash_lookup(obj.id());
            if (dst_thread_id != 0) {
                migrate_channel.migrate(obj, dst_thread_id);
            }
        }
        src_list.test_deletion_finalize();
        migrate_channel.flush();
        migrate_channel.prepare_immigrants();
        src_list.test_sort();

        // Push
        auto push_channel = create_push_combined_channel<int, SumCombiner<int>>(src_list, src_list);
        push_channel.setup(0, 0, &workerinfo, &mailbox_0, &hashring);
        push_channel.push(123, 1);
        push_channel.push(123, 1342148);
        push_channel.push(123, 5);
        push_channel.push(123, 100);
        push_channel.push(123, 18);
        push_channel.push(123, 57);

        push_channel.flush();
        push_channel.prepare_messages();

        for (auto& obj : src_list.get_data()) {
            auto msg = push_channel.get(obj);
            EXPECT_EQ(msg, 246);
        }
    });
    std::thread th2 = std::thread([&]() {
        ObjListForTest<Obj> src_list;

        src_list.add_object(Obj(1));
        src_list.add_object(Obj(1342148));
        src_list.add_object(Obj(5));

        // GLobalize
        auto migrate_channel = create_migrate_channel(src_list, src_list);
        migrate_channel.setup(1, 1, &workerinfo, &mailbox_1, &hashring);
        for (auto& obj : src_list.get_data()) {
            int dst_thread_id = hashring.hash_lookup(obj.id());
            if (dst_thread_id != 1) {
                migrate_channel.migrate(obj, dst_thread_id);
            }
        }
        src_list.test_deletion_finalize();
        migrate_channel.flush();
        migrate_channel.prepare_immigrants();
        src_list.test_sort();

        // Push
        auto push_channel = create_push_combined_channel<int, SumCombiner<int>>(src_list, src_list);
        push_channel.setup(1, 1, &workerinfo, &mailbox_1, &hashring);
        push_channel.push(123, 1);
        push_channel.push(123, 1342148);
        push_channel.push(123, 5);
        push_channel.push(123, 100);
        push_channel.push(123, 18);
        push_channel.push(123, 57);

        push_channel.flush();
        push_channel.prepare_messages();

        for (auto& obj : src_list.get_data()) {
            auto msg = push_channel.get(obj);
            EXPECT_EQ(msg, 246);
        }
    });

    th1.join();
    th2.join();
}

}  // namespace
}  // namespace husky
