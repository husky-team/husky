#include "core/channel/channel_factory_base.hpp"

#include "gtest/gtest.h"

#include "core/combiner.hpp"
#include "core/objlist.hpp"
#include "core/objlist_factory.hpp"

namespace husky {
namespace {

class TestChannelFactoryBaseH3 : public testing::Test {
   public:
    TestChannelFactoryBaseH3() {}
    ~TestChannelFactoryBaseH3() {}

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

TEST_F(TestChannelFactoryBaseH3, CreatePushChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // Create
    auto& ch1 = ChannelFactoryBase::create_push_channel<int>(src_list, dst_list, "ch1");
    EXPECT_EQ(ChannelFactoryBase::size(), 1);
    auto& ch2 = ChannelFactoryBase::create_push_channel<int>(src_list, dst_list, "ch2");
    EXPECT_EQ(ChannelFactoryBase::size(), 2);

    // Get
    auto& ch3 = ChannelFactoryBase::get_push_channel<int, Obj>("ch1");
    EXPECT_EQ(&ch3, &ch1);
    auto& ch4 = ChannelFactoryBase::get_push_channel<int, Obj>("ch2");
    EXPECT_EQ(&ch4, &ch2);

    // Drop
    ChannelFactoryBase::drop_channel("ch1");
    EXPECT_EQ(ChannelFactoryBase::size(), 1);
    ChannelFactoryBase::drop_channel("ch2");
    EXPECT_EQ(ChannelFactoryBase::size(), 0);
}

TEST_F(TestChannelFactoryBaseH3, CreatePushCombinedChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // Create
    auto& ch1 = ChannelFactoryBase::create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list, "ch1");
    EXPECT_EQ(ChannelFactoryBase::size(), 1);
    auto& ch2 = ChannelFactoryBase::create_push_combined_channel<int, SumCombiner<int>>(src_list, dst_list, "ch2");
    EXPECT_EQ(ChannelFactoryBase::size(), 2);

    // Get
    auto& ch3 = ChannelFactoryBase::get_push_combined_channel<int, SumCombiner<int>, Obj>("ch1");
    EXPECT_EQ(&ch3, &ch1);
    auto& ch4 = ChannelFactoryBase::get_push_combined_channel<int, SumCombiner<int>, Obj>("ch2");
    EXPECT_EQ(&ch4, &ch2);

    // Drop
    ChannelFactoryBase::drop_channel("ch1");
    EXPECT_EQ(ChannelFactoryBase::size(), 1);
    ChannelFactoryBase::drop_channel("ch2");
    EXPECT_EQ(ChannelFactoryBase::size(), 0);
}

TEST_F(TestChannelFactoryBaseH3, CreateMigrateChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // Create
    auto& ch1 = ChannelFactoryBase::create_migrate_channel<Obj>(src_list, dst_list, "ch1");
    EXPECT_EQ(ChannelFactoryBase::size(), 1);
    auto& ch2 = ChannelFactoryBase::create_migrate_channel<Obj>(src_list, dst_list, "ch2");
    EXPECT_EQ(ChannelFactoryBase::size(), 2);

    // Get
    auto& ch3 = ChannelFactoryBase::get_migrate_channel<Obj>("ch1");
    EXPECT_EQ(&ch3, &ch1);
    auto& ch4 = ChannelFactoryBase::get_migrate_channel<Obj>("ch2");
    EXPECT_EQ(&ch4, &ch2);

    // Drop
    ChannelFactoryBase::drop_channel("ch1");
    EXPECT_EQ(ChannelFactoryBase::size(), 1);
    ChannelFactoryBase::drop_channel("ch2");
    EXPECT_EQ(ChannelFactoryBase::size(), 0);
}

TEST_F(TestChannelFactoryBaseH3, CreateBroacastChannel) {
    ObjList<Obj> src_list;
    ObjList<Obj> dst_list;

    // Create
    auto& ch1 = ChannelFactoryBase::create_broadcast_channel<int, int>(src_list, "ch1");
    EXPECT_EQ(ChannelFactoryBase::size(), 1);
    auto& ch2 = ChannelFactoryBase::create_broadcast_channel<int, int>(src_list, "ch2");
    EXPECT_EQ(ChannelFactoryBase::size(), 2);

    // Get
    auto& ch3 = ChannelFactoryBase::get_broadcast_channel<int, int>("ch1");
    EXPECT_EQ(&ch3, &ch1);
    auto& ch4 = ChannelFactoryBase::get_broadcast_channel<int, int>("ch2");
    EXPECT_EQ(&ch4, &ch2);

    // Drop
    ChannelFactoryBase::drop_channel("ch1");
    EXPECT_EQ(ChannelFactoryBase::size(), 1);
    ChannelFactoryBase::drop_channel("ch2");
    EXPECT_EQ(ChannelFactoryBase::size(), 0);
}

}  // namespace
}  // namespace husky
