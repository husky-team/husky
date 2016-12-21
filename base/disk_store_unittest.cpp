#include "base/disk_store.hpp"

#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "base/serialization.hpp"

namespace husky {
namespace {

using base::BinStream;
using base::DiskStore;

class TestDiskStore : public testing::Test {
   public:
    TestDiskStore() {}
    ~TestDiskStore() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestDiskStore, WriteAndRead) {
    DiskStore ds("TestDiskStore.WriteAndRead");

    std::vector<char> to;
    for (int i = 0; i < 1024; i++)
        to.push_back(static_cast<char>(i));
    BinStream bs_to(to);
    EXPECT_TRUE(ds.write(std::move(bs_to)));

    BinStream bs_from = ds.read();
    EXPECT_NE(bs_from.size(), 0);
    std::vector<char> from = bs_from.get_buffer_vector();

    EXPECT_EQ(from.size(), to.size());
    for (int i = 0; i < 1024; i++)
        EXPECT_EQ(from[i], to[i]);
}

}  // namespace
}  // namespace husky
