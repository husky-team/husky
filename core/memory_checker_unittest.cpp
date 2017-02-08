#include "core/memory_checker.hpp"

#include "gtest/gtest.h"

namespace husky {
namespace {

class TestMemoryChecker : public testing::Test {
   public:
    TestMemoryChecker() {}
    ~TestMemoryChecker() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestMemoryChecker, MemoryInfoInitialization) {
    MemoryInfo info = MemoryChecker::get_memory_info();
    EXPECT_EQ(info.allocated_bytes, 0);
    EXPECT_EQ(info.heap_size, 0);
}

}  // namespace
}  // namespace husky
