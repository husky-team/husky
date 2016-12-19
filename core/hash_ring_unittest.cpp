#include "core/hash_ring.hpp"

#include <iostream>
#include <set>
#include <vector>

#include "gtest/gtest.h"

#include "base/serialization.hpp"

namespace husky {
namespace {

using base::BinStream;

class TestHashRing : public testing::Test {
   public:
    TestHashRing() {}
    ~TestHashRing() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestHashRing, InitAndDelete) {
    HashRing* hash_ring = new HashRing();
    ASSERT_TRUE(hash_ring != NULL);
    delete hash_ring;
}

TEST_F(TestHashRing, Functional) {
    HashRing hash_ring;
    ASSERT_EQ(hash_ring.get_global_tids_size(), 0);

    hash_ring.insert(0);
    hash_ring.insert(1);
    hash_ring.insert(2);
    EXPECT_EQ(hash_ring.get_global_tids_size(), 3);

    hash_ring.remove(3);
    EXPECT_EQ(hash_ring.get_global_tids_size(), 3);

    hash_ring.remove(1);
    EXPECT_EQ(hash_ring.get_global_tids_size(), 2);

    hash_ring.insert(3);
    hash_ring.insert(4);
    hash_ring.insert(5);
    EXPECT_EQ(hash_ring.get_global_tids_size(), 5);

    std::vector<int> locations = {0, 0, 4, 4, 2, 5, 3, 0, 5, 3, 3, 3, 2, 0, 0, 5, 3, 5, 5, 5};
    for (int i = 0; i < 20; i++)
        EXPECT_EQ(hash_ring.lookup(i), locations[i]);
}

TEST_F(TestHashRing, FunctionalWithVirtualNodes) {
    int virtual_nodes = 2;
    HashRing hash_ring;
    ASSERT_EQ(hash_ring.get_global_tids_size(), 0);

    hash_ring.insert(0, virtual_nodes);
    hash_ring.insert(1, virtual_nodes);
    hash_ring.insert(2, virtual_nodes);
    EXPECT_EQ(hash_ring.get_global_tids_size(), 3 * virtual_nodes);

    hash_ring.remove(3);
    EXPECT_EQ(hash_ring.get_global_tids_size(), 3 * virtual_nodes);

    hash_ring.remove(1);
    EXPECT_EQ(hash_ring.get_global_tids_size(), 2 * virtual_nodes);

    hash_ring.insert(3, virtual_nodes);
    hash_ring.insert(4, virtual_nodes);
    hash_ring.insert(5, virtual_nodes);
    EXPECT_EQ(hash_ring.get_global_tids_size(), 5 * virtual_nodes);

    std::vector<int> locations = {0, 4, 4, 5, 0, 3, 5, 0, 3, 4, 4, 3, 0, 0, 5, 4, 2, 5, 3, 4};
    for (int i = 0; i < 20; i++)
        EXPECT_EQ(hash_ring.lookup(i), locations[i]);
}

TEST_F(TestHashRing, Serialization) {
    HashRing input, output;
    BinStream stream;
    stream << input;
    stream >> output;
    EXPECT_EQ(input.get_global_tids_size(), output.get_global_tids_size());

    input.insert(5);
    input.insert(15);
    input.insert(25);

    stream << input;
    stream >> output;
    EXPECT_EQ(input.get_global_tids_size(), output.get_global_tids_size());
}

}  // namespace
}  // namespace husky
