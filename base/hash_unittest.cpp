#include "base/hash.hpp"

#include <functional>
#include <string>
#include <utility>

#include "gtest/gtest.h"

typedef std::string String;
typedef std::pair<int, int> PairInt;
typedef std::pair<std::string, std::string> PairString;
typedef std::hash<int> HashInt;
typedef std::hash<std::string> HashString;
typedef std::hash<PairInt> HashPairInt;
typedef std::hash<PairString> HashPairString;

namespace husky {
namespace {

class TestHash : public testing::Test {
   public:
    TestHash() {}
    ~TestHash() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestHash, InitAndDelete) {
    HashPairInt* hash_pair_int = new HashPairInt();
    HashPairString* hash_pair_string = new HashPairString();
    ASSERT_TRUE(hash_pair_int != NULL);
    ASSERT_TRUE(hash_pair_string != NULL);
    delete hash_pair_int;
    delete hash_pair_string;
}

TEST_F(TestHash, CheckHashValue) {
    int ia = 12, ib = 24;
    PairInt pair_int = std::make_pair(12, 24);
    HashInt hash_int;
    HashPairInt hash_pair_int;
    EXPECT_EQ(hash_pair_int(pair_int), hash_int(12) ^ hash_int(24));

    String sa = "aa";
    String sb = "bb";
    HashString hash_string;
    PairString pair_string = std::make_pair(sa, sb);
    HashPairString hash_pair_string;
    EXPECT_EQ(hash_pair_string(pair_string), hash_string(sa) ^ hash_string(sb));
}

}  // namespace
}  // namespace husky
