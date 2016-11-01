#include "core/channel/migrate_channel.hpp"

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

class TestMigrateChannel : public testing::Test {
   public:
    TestMigrateChannel() {}
    ~TestMigrateChannel() {}

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
    Attr(const Attr&) = default;
    Attr& operator=(const Attr&) = default;
    Attr(Attr&&) = default;
    Attr& operator=(Attr&&) = default;

    explicit Attr(std::string&& s) : str(std::move(s)) {}
    std::string str;

    friend husky::BinStream& operator<<(husky::BinStream& bin, const Attr& attr) {
        bin << attr.str;
        return bin;
    }
    friend husky::BinStream& operator>>(husky::BinStream& bin, Attr& attr) {
        bin >> attr.str;
        return bin;
    }
};

// Create MigrateChannel without setting, for setup
template <typename ObjT>
MigrateChannel<ObjT> create_migrate_channel(ObjList<ObjT>& src_list, ObjList<ObjT>& dst_list) {
    MigrateChannel<ObjT> migrate_channel(&src_list, &dst_list);
    return migrate_channel;
}

TEST_F(TestMigrateChannel, Create) {
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
    workerinfo.add_proc(0, "worker1");
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_num_processes(1);
    workerinfo.set_num_workers(1);
    workerinfo.set_proc_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // MigrateChannel
    auto migrate_channel = create_migrate_channel(src_list, dst_list);
    migrate_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
}

TEST_F(TestMigrateChannel, MigrateOther) {
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
    workerinfo.add_proc(0, "worker1");
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_num_processes(1);
    workerinfo.set_num_workers(1);
    workerinfo.set_proc_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    src_list.add_object(Obj(100));
    src_list.add_object(Obj(18));
    src_list.add_object(Obj(57));

    // MigrateChannel
    auto migrate_channel = create_migrate_channel(src_list, dst_list);
    migrate_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // migrate
    Obj* p = src_list.find(18);
    migrate_channel.migrate(*p, 0);  // migrate Obj(18) to thread 0
    migrate_channel.flush();
    // migration done
    migrate_channel.prepare_immigrants();
    Obj& obj = dst_list.get_data()[0];

    EXPECT_EQ(obj.id(), 18);
    EXPECT_EQ(dst_list.get_size(), 1);
    EXPECT_EQ(src_list.get_size(), 2);
}

TEST_F(TestMigrateChannel, MigrateItself) {
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
    workerinfo.add_proc(0, "worker1");
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_num_processes(1);
    workerinfo.set_num_workers(1);
    workerinfo.set_proc_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;
    auto& scr_int = src_list.create_attrlist<int>("int");
    auto& src_attr = src_list.create_attrlist<Attr>("attr");
    dst_list.create_attrlist<int>("int");
    dst_list.create_attrlist<Attr>("attr");

    auto idx = src_list.add_object(Obj(100));
    scr_int.set(idx, 100);
    src_attr.set(idx, Attr("100"));
    idx = src_list.add_object(Obj(18));
    scr_int.set(idx, 18);
    src_attr.set(idx, Attr("18"));
    idx = src_list.add_object(Obj(57));
    scr_int.set(idx, 57);
    src_attr.set(idx, Attr("57"));

    // MigrateChannel
    auto migrate_channel = create_migrate_channel(src_list, dst_list);
    migrate_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // migrate
    Obj* p = src_list.find(18);
    migrate_channel.migrate(*p, 0);  // migrate Obj(18) to thread 0
    migrate_channel.flush();
    // migration done
    migrate_channel.prepare_immigrants();
    Obj& obj = dst_list.get_data()[0];
    auto& dst_int = dst_list.get_attrlist<int>("int");
    auto& dst_attr = dst_list.get_attrlist<Attr>("attr");

    EXPECT_EQ(src_list.get_size(), 2);
    EXPECT_EQ(dst_list.get_size(), 1);
    EXPECT_EQ(obj.id(), 18);
    EXPECT_EQ(dst_int.get(obj), 18);
    EXPECT_STREQ(dst_attr.get(obj).str.c_str(), "18");
}

TEST_F(TestMigrateChannel, MigrateOtherIncProgress) {
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
    workerinfo.add_proc(0, "worker1");
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_num_processes(1);
    workerinfo.set_num_workers(1);
    workerinfo.set_proc_id(0);

    // ObjList Setup
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;
    auto& scr_int = src_list.create_attrlist<int>("int");
    auto& src_attr = src_list.create_attrlist<Attr>("attr");
    dst_list.create_attrlist<int>("int");
    dst_list.create_attrlist<Attr>("attr");

    auto idx = src_list.add_object(Obj(100));
    scr_int.set(idx, 100);
    src_attr.set(idx, Attr("100"));
    idx = src_list.add_object(Obj(18));
    scr_int.set(idx, 18);
    src_attr.set(idx, Attr("18"));
    idx = src_list.add_object(Obj(57));
    scr_int.set(idx, 57);
    src_attr.set(idx, Attr("57"));

    // MigrateChannel
    // Round 1
    auto migrate_channel = create_migrate_channel(src_list, dst_list);
    migrate_channel.setup(0, 0, &workerinfo, &mailbox, &hashring);
    // migrate
    Obj* p = src_list.find(18);
    migrate_channel.migrate(*p, 0);  // migrate Obj(18) to thread 0
    migrate_channel.flush();
    // migration done
    migrate_channel.prepare_immigrants();
    Obj& obj = dst_list.get_data()[0];
    auto& dst_int = dst_list.get_attrlist<int>("int");
    auto& dst_attr = dst_list.get_attrlist<Attr>("attr");

    EXPECT_EQ(obj.id(), 18);
    EXPECT_EQ(dst_list.get_size(), 1);
    EXPECT_EQ(src_list.get_size(), 2);
    EXPECT_EQ(dst_int.get(obj), 18);
    EXPECT_STREQ(dst_attr.get(obj).str.c_str(), "18");

    // Round 2
    // migrate
    p = src_list.find(100);
    migrate_channel.migrate(*p, 0);  // migrate Obj(100) to thread 0
    migrate_channel.flush();
    // migration done
    migrate_channel.prepare_immigrants();

    EXPECT_EQ(dst_list.get_size(), 2);
    EXPECT_EQ(src_list.get_size(), 1);
}

}  // namespace
}  // namespace husky
