#include "core/channel/channel_store_base.hpp"

#include "gtest/gtest.h"

#include "core/combiner.hpp"
#include "core/objlist.hpp"
#include "core/objlist_store.hpp"

namespace husky {
namespace {

class TestChannelStoreBase : public testing::Test {
   public:
    TestChannelStoreBase() {}
    ~TestChannelStoreBase() {}

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

TEST_F(TestChannelStoreBase, CreatePushChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // Create
    auto& ch1 = ChannelStoreBase::create_push_channel<int>(src_list, dst_list, "ch1");
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_push_channel<int>(src_list, dst_list, "ch2");
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    // Get
    auto& ch3 = ChannelStoreBase::get_push_channel<int, Obj>("ch1");
    EXPECT_EQ(&ch3, &ch1);
    auto& ch4 = ChannelStoreBase::get_push_channel<int, Obj>("ch2");
    EXPECT_EQ(&ch4, &ch2);

    // Drop
    ChannelStoreBase::drop_channel("ch1");
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel("ch2");
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

TEST_F(TestChannelStoreBase, CreatePushCombinedChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // Create
    auto& ch1 = ChannelStoreBase::create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list, "ch1");
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list, "ch2");
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    // Get
    auto& ch3 = ChannelStoreBase::get_push_combined_channel<int, SumCombiner<int>, Obj>("ch1");
    EXPECT_EQ(&ch3, &ch1);
    auto& ch4 = ChannelStoreBase::get_push_combined_channel<int, SumCombiner<int>, Obj>("ch2");
    EXPECT_EQ(&ch4, &ch2);

    // Drop
    ChannelStoreBase::drop_channel("ch1");
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel("ch2");
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

TEST_F(TestChannelStoreBase, CreateMigrateChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // Create
    auto& ch1 = ChannelStoreBase::create_migrate_channel<Obj>(src_list, dst_list, "ch1");
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_migrate_channel<Obj>(src_list, dst_list, "ch2");
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    // Get
    auto& ch3 = ChannelStoreBase::get_migrate_channel<Obj>("ch1");
    EXPECT_EQ(&ch3, &ch1);
    auto& ch4 = ChannelStoreBase::get_migrate_channel<Obj>("ch2");
    EXPECT_EQ(&ch4, &ch2);

    // Drop
    ChannelStoreBase::drop_channel("ch1");
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel("ch2");
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

TEST_F(TestChannelStoreBase, CreateBroacastChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // Create
    auto& ch1 = ChannelStoreBase::create_broadcast_channel<int, int>(src_list, "ch1");
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_broadcast_channel<int, int>(src_list, "ch2");
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    // Get
    auto& ch3 = ChannelStoreBase::get_broadcast_channel<int, int>("ch1");
    EXPECT_EQ(&ch3, &ch1);
    auto& ch4 = ChannelStoreBase::get_broadcast_channel<int, int>("ch2");
    EXPECT_EQ(&ch4, &ch2);

    // Drop
    ChannelStoreBase::drop_channel("ch1");
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel("ch2");
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

}  // namespace
}  // namespace husky
