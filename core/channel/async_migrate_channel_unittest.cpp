#include "core/channel/async_migrate_channel.hpp"

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

class TestAsyncMigrateChannel : public testing::Test {
   public:
    TestAsyncMigrateChannel() {}
    ~TestAsyncMigrateChannel() {}

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

class Attr {
   public:
    Attr() = default;
    ~Attr() = default;
    explicit Attr(std::string&& s) : str(std::move(s)) {}
    std::string str;
};

// Create AsyncMigrateChannel without setting, for setup
template <typename ObjT>
AsyncMigrateChannel<ObjT> create_async_migrate_channel(ObjList<ObjT>& obj_list) {
    AsyncMigrateChannel<ObjT> async_migrate_channel(&obj_list);
    return async_migrate_channel;
}

TEST_F(TestAsyncMigrateChannel, Create) {
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

    // AsyncMigrateChannel
    auto async_migrate_channel = create_async_migrate_channel(obj_list);
    async_migrate_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
}

TEST_F(TestAsyncMigrateChannel, MigrateOtherIncProgress) {
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

    auto& scr_int = obj_list.create_attrlist<int>("int");
    auto& src_attr = obj_list.create_attrlist<Attr>("attr");

    auto idx = obj_list.add_object(Obj(100));
    scr_int.set(idx, 100);
    src_attr.set(idx, Attr("100"));

    // AsyncMigrateChannel
    // Round 1
    auto async_migrate_channel = create_async_migrate_channel(obj_list);
    async_migrate_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // migrate
    Obj* p = obj_list.find(100);
    async_migrate_channel.migrate(*p, 0);  // migrate Obj(18) to thread 0
    async_migrate_channel.out();
    obj_list.deletion_finalize();
    // migration done
    while (
        mailbox.poll_with_timeout(async_migrate_channel.get_channel_id(), async_migrate_channel.get_progress(), 1.0)) {
        auto bin_push = mailbox.recv(async_migrate_channel.get_channel_id(), async_migrate_channel.get_progress());
        async_migrate_channel.in(bin_push);
    }
    Obj& obj = obj_list.get_data()[0];

    EXPECT_EQ(obj.id(), 100);
    EXPECT_EQ(obj_list.get_size(), 1);
    EXPECT_EQ(scr_int.get(obj), 100);
    EXPECT_STREQ(src_attr.get(obj).str.c_str(), "100");

    // Round 2
    idx = obj_list.add_object(Obj(18));
    idx = obj_list.add_object(Obj(57));

    // migrate
    p = obj_list.find(18);
    async_migrate_channel.migrate(*p, 0);  // migrate Obj(100) to thread 0
    p = obj_list.find(57);
    async_migrate_channel.migrate(*p, 0);  // migrate Obj(57) to thread 0
    async_migrate_channel.out();
    obj_list.deletion_finalize();
    // migration done
    while (
        mailbox.poll_with_timeout(async_migrate_channel.get_channel_id(), async_migrate_channel.get_progress(), 1.0)) {
        auto bin_push = mailbox.recv(async_migrate_channel.get_channel_id(), async_migrate_channel.get_progress());
        async_migrate_channel.in(bin_push);
    }
    EXPECT_EQ(obj_list.get_size(), 3);
}

}  // namespace
}  // namespace husky
