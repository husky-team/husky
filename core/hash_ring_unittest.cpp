#include "core/hash_ring.hpp"

#include <set>
#include <vector>

#include "gtest/gtest.h"

#include "base/serialization.hpp"

namespace husky {

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
    ASSERT_EQ(hash_ring.get_num_workers(), 0);

    hash_ring.insert(0);
    hash_ring.insert(1);
    hash_ring.insert(2);
    EXPECT_EQ(hash_ring.get_num_workers(), 3);

    hash_ring.remove(3);
    EXPECT_EQ(hash_ring.get_num_workers(), 3);

    hash_ring.remove(1);
    EXPECT_EQ(hash_ring.get_num_workers(), 2);

    hash_ring.insert(3);
    hash_ring.insert(4);
    hash_ring.insert(5);
    EXPECT_EQ(hash_ring.get_num_workers(), 5);

    // TODO(anyone): how to validate `lookup` locations.
    // Locations for number of workers 5.
    std::vector<int> locations = {0, 0, 3, 3, 1, 4, 2, 0, 4, 2, 2, 2, 1, 0, 0, 4, 2, 4, 4, 4};
    for (int i = 0; i < 20; i++)
        EXPECT_EQ(hash_ring.lookup(i), locations[i]);
}

TEST_F(TestHashRing, Serialization) {
    HashRing input, output;
    BinStream stream;
    stream << input;
    stream >> output;
    EXPECT_EQ(input.get_num_workers(), output.get_num_workers());

    input.insert(5);
    input.insert(15);
    input.insert(25);

    stream << input;
    stream >> output;
    EXPECT_EQ(input.get_num_workers(), output.get_num_workers());
}

}  // namespace husky
