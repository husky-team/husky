#include "base/serialization.hpp"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace husky {
namespace {

using base::BinStream;

class TestSerialization : public testing::Test {
   public:
    TestSerialization() {}
    ~TestSerialization() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class TestSerializationWithInheritance {
   public:
    TestSerializationWithInheritance(int y) : x(y) {}

    BinStream& serialize(BinStream& stream) const { return stream << x; }

    BinStream& deserialize(BinStream& stream) { return stream >> x; }

    bool operator==(const TestSerializationWithInheritance& rhs) const { return x == rhs.x; }

   protected:
    int x;
};

class TestSerializationWithInheritanceSubclass : public TestSerializationWithInheritance {
   public:
    TestSerializationWithInheritanceSubclass(int y) : TestSerializationWithInheritance(y) {}
};

TEST_F(TestSerialization, InitAndDelete) {
    BinStream* stream = new BinStream();
    ASSERT_TRUE(stream != NULL);
    delete stream;
}

TEST_F(TestSerialization, BoolType) {
    bool input = true;
    BinStream stream;
    stream << input;
    EXPECT_EQ(stream.size(), 1);
    bool output = false;
    stream >> output;
    EXPECT_EQ(output, input);
}

TEST_F(TestSerialization, CharType) {
    char input = 'a';
    BinStream stream;
    stream << input;
    EXPECT_EQ(stream.size(), 1);
    char output = 'b';
    stream >> output;
    EXPECT_EQ(output, input);
}

TEST_F(TestSerialization, IntegerType) {
    int input = 1001;
    BinStream stream;
    stream << input;
    EXPECT_EQ(stream.size(), 4);
    int output = 0;
    stream >> output;
    EXPECT_EQ(output, input);
}

TEST_F(TestSerialization, FloatType) {
    float input = .123;
    BinStream stream;
    stream << input;
    EXPECT_EQ(stream.size(), 4);
    float output = .0;
    stream >> output;
    EXPECT_FLOAT_EQ(output, input);
}

TEST_F(TestSerialization, DoubleType) {
    double input = .123456;
    BinStream stream;
    stream << input;
    EXPECT_EQ(stream.size(), 8);
    double output = .0;
    stream >> output;
    EXPECT_DOUBLE_EQ(output, input);
}

TEST_F(TestSerialization, EmptyString) {
    std::string input, output;
    BinStream stream;
    stream << input;
    stream >> output;
    EXPECT_STREQ(output.c_str(), input.c_str());
}

TEST_F(TestSerialization, String) {
    std::string input = "123abc";
    BinStream stream;
    stream << input;
    std::string output;
    stream >> output;
    EXPECT_STREQ(output.c_str(), input.c_str());
}

TEST_F(TestSerialization, EmptyVector) {
    std::vector<int> input, output;
    BinStream stream;
    stream << input;
    stream >> output;
    EXPECT_EQ(output.size(), input.size());
}

TEST_F(TestSerialization, Vector) {
    std::vector<int> input{1, 0, -2, 3};
    BinStream stream;
    stream << input;
    std::vector<int> output;
    stream >> output;
    EXPECT_EQ(output.size(), input.size());
    for (int i = 0; i < output.size(); i++)
        EXPECT_EQ(output[i], input[i]);
}

TEST_F(TestSerialization, VectorBool) {
    std::vector<bool> input{1, 0, 1};
    BinStream stream;
    stream << input;
    std::vector<bool> output;
    stream >> output;
    EXPECT_EQ(output.size(), input.size());
    for (int i = 0; i < output.size(); i++)
        EXPECT_EQ(output[i], input[i]);
}

TEST_F(TestSerialization, Pair) {
    std::vector<int> itemVector{2, 2, 2, 2};
    BinStream stream;
    std::pair<int, std::vector<int>> input(1, itemVector);
    stream << input;
    std::pair<int, std::vector<int>> output;
    stream >> output;
    EXPECT_EQ(output.first, input.first);
    for (int i = 0; i < output.second.size(); i++)
        EXPECT_EQ(output.second[i], input.second[i]);
}

TEST_F(TestSerialization, PairVector) {
    std::vector<int> itemVector{2, 2, 2, 2};
    std::pair<int, std::vector<int>> p1(1, itemVector);
    std::pair<int, std::vector<int>> p2(2, itemVector);
    std::vector<std::pair<int, std::vector<int>>> input;
    input.push_back(p1);
    input.push_back(p2);

    BinStream stream;
    stream << input;
    std::vector<std::pair<int, std::vector<int>>> output;
    stream >> output;

    EXPECT_EQ(output.size(), input.size());
    for (int i = 0; i < output.size(); ++i)
        EXPECT_EQ(output[i], input[i]);
}

TEST_F(TestSerialization, TestSharedPtr) {
    auto input = std::make_shared<int>(100);
    std::shared_ptr<int> output(new int);
    BinStream stream;
    stream << input;
    stream >> output;
    EXPECT_EQ(*input, *output);
}

TEST_F(TestSerialization, TestUniquePtr) {
    auto input = std::make_unique<int>(100);
    std::unique_ptr<int> output(new int);
    BinStream stream;
    stream << input;
    stream >> output;
    EXPECT_EQ(*input, *output);
}

TEST_F(TestSerialization, EmptyMap) {
    std::map<int, int> input, output;
    BinStream stream;
    stream << input;
    stream >> output;
    EXPECT_EQ(output.size(), input.size());
}

TEST_F(TestSerialization, Map) {
    std::map<int, int> input, output;
    input.insert(std::pair<int, int>(1, 2));
    input.insert(std::pair<int, int>(3, 4));
    BinStream stream;
    stream << input;
    stream >> output;
    EXPECT_EQ(input.size(), output.size());
    std::map<int, int>::iterator iter;
    iter = output.find(1);
    EXPECT_EQ(iter->second, 2);
    iter = output.find(3);
    EXPECT_EQ(iter->second, 4);
}

TEST_F(TestSerialization, OtherBinStream) {
    std::string s = "123abc";
    BinStream stream;
    stream << s;
    BinStream input, output;
    input << stream;
    input >> output;
    EXPECT_EQ(stream.size(), output.size());
    std::string ss;
    output >> ss;
    EXPECT_STREQ(s.c_str(), ss.c_str());
}

TEST_F(TestSerialization, Mixture) {
    bool a = true;
    std::string b = "husky";
    double c = .343412;
    int d = 65535;
    BinStream stream;
    stream << a << b << c << d;
    bool aa;
    std::string bb;
    double cc;
    int dd;
    stream >> aa >> bb >> cc >> dd;
    EXPECT_EQ(a, aa);
    EXPECT_STREQ(b.c_str(), bb.c_str());
    EXPECT_EQ(c, cc);
    EXPECT_EQ(d, dd);
}

TEST_F(TestSerialization, UsingMemberFunction) {
    BinStream stream;
    TestSerializationWithInheritance a(123);
    stream << a;
    TestSerializationWithInheritance b(0);
    stream >> b;
    EXPECT_EQ(a, b);
}

TEST_F(TestSerialization, WithInheritance) {
    BinStream stream;
    TestSerializationWithInheritanceSubclass a(123);
    stream << a;
    TestSerializationWithInheritanceSubclass b(0);
    stream >> b;
    EXPECT_EQ(a, b);
}

}  // namespace
}  // namespace husky
