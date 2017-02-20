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

    auto& ch1 = ChannelStoreBase::create_push_channel<int>(src_list, dst_list);
    size_t ch1_id = ch1.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_push_channel<int>(src_list, dst_list);
    size_t ch2_id = ch2.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    ChannelStoreBase::drop_channel(ch2_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel(ch1_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

TEST_F(TestChannelStoreBase, CreatePushCombinedChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    auto& ch1 = ChannelStoreBase::create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list);
    size_t ch1_id = ch1.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list);
    size_t ch2_id = ch2.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    ChannelStoreBase::drop_channel(ch2_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel(ch1_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

TEST_F(TestChannelStoreBase, CreateMigrateChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    auto& ch1 = ChannelStoreBase::create_migrate_channel<Obj>(src_list, dst_list);
    size_t ch1_id = ch1.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_migrate_channel<Obj>(src_list, dst_list);
    size_t ch2_id = ch2.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    ChannelStoreBase::drop_channel(ch2_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel(ch1_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

TEST_F(TestChannelStoreBase, CreateBroacastChannel) {
    ObjList<Obj> src_list;

    auto& ch1 = ChannelStoreBase::create_broadcast_channel<int, int>(src_list);
    size_t ch1_id = ch1.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_broadcast_channel<int, int>(src_list);
    size_t ch2_id = ch2.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    ChannelStoreBase::drop_channel(ch2_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel(ch1_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

TEST_F(TestChannelStoreBase, CreateAsyncPushChannel) {
    ObjList<Obj> src_list;

    auto& ch1 = ChannelStoreBase::create_async_push_channel<int, Obj>(src_list);
    size_t ch1_id = ch1.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_async_push_channel<int, Obj>(src_list);
    size_t ch2_id = ch2.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    ChannelStoreBase::drop_channel(ch2_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel(ch1_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

TEST_F(TestChannelStoreBase, CreateAsyncMigrateChannel) {
    ObjList<Obj> src_list;

    auto& ch1 = ChannelStoreBase::create_async_migrate_channel<Obj>(src_list);
    size_t ch1_id = ch1.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    auto& ch2 = ChannelStoreBase::create_async_migrate_channel<Obj>(src_list);
    size_t ch2_id = ch2.get_channel_id();
    EXPECT_TRUE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 2);

    ChannelStoreBase::drop_channel(ch2_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch2_id));
    EXPECT_EQ(ChannelStoreBase::size(), 1);
    ChannelStoreBase::drop_channel(ch1_id);
    EXPECT_FALSE(ChannelStoreBase::has_channel(ch1_id));
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

TEST_F(TestChannelStoreBase, Functional) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    ChannelStoreBase::create_push_channel<int>(src_list, dst_list);
    ChannelStoreBase::create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list);
    ChannelStoreBase::create_migrate_channel<Obj>(src_list, dst_list);
    ChannelStoreBase::create_broadcast_channel<int, int>(src_list);
    ChannelStoreBase::create_async_push_channel<int, Obj>(src_list);
    ChannelStoreBase::create_async_migrate_channel<Obj>(src_list);
    EXPECT_EQ(ChannelStoreBase::size(), 6);

    ChannelStoreBase::drop_all_channels();
    EXPECT_EQ(ChannelStoreBase::size(), 0);
}

}  // namespace
}  // namespace husky
