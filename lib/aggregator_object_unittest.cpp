#include "lib/aggregator_object.hpp"

#include "gtest/gtest.h"

#include "base/serialization.hpp"

class TestCopyAssign : public testing::Test {
   public:
    TestCopyAssign() {}
    ~TestCopyAssign() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class TestAggregatorState : public testing::Test {
   public:
    TestAggregatorState() {}
    ~TestAggregatorState() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class TestAggregatorObject : public testing::Test {
   public:
    TestAggregatorObject() {}
    ~TestAggregatorObject() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class SameType {
   public:
    explicit SameType(int val = 0) : some_mem(val) {}
    friend SameType operator+(SameType a, SameType b) { return SameType(a.some_mem + b.some_mem); }
    SameType& operator=(SameType c) {
        some_mem = c.some_mem;
        return *this;
    }

    int some_mem;
};

class DiffType;

class DiffSumType {
   public:
    DiffSumType(const DiffType* x, const DiffType* y) : a(x), b(y) {}

    const DiffType* a;
    const DiffType* b;
};

class DiffType {
   public:
    explicit DiffType(int val = 0) : some_mem(val) {}
    friend DiffSumType operator+(const DiffType& a, const DiffType& b) { return DiffSumType(&a, &b); }
    DiffType& operator=(DiffSumType x) {
        some_mem = x.a->some_mem + x.b->some_mem;
        return *this;
    }

    int some_mem;
};

TEST_F(TestCopyAssign, SameTypeTest) {
    SameType a(2333), b(7086), c;
    husky::lib::copy_assign(c, a + b);
    EXPECT_EQ(a.some_mem + b.some_mem, c.some_mem);
}

TEST_F(TestCopyAssign, DiffTypeTest) {
    DiffType a(9527), b(709394), c;
    husky::lib::copy_assign(c, a + b);
    EXPECT_EQ(a.some_mem + b.some_mem, c.some_mem);
}

class SomeAggregatorState : public husky::lib::AggregatorState {
   public:
    void load(husky::base::BinStream& in) {}
    void save(husky::base::BinStream& out) {}
    void aggregate(husky::lib::AggregatorState* b) {}
    void prepare_value() {}
};

TEST_F(TestAggregatorState, Basic) {
    SomeAggregatorState s;

    s.set_zero();
    EXPECT_TRUE(s.is_zero());
    s.set_non_zero();
    EXPECT_FALSE(s.is_zero());

    s.set_updated();
    EXPECT_TRUE(s.is_updated());
    s.set_non_updated();
    EXPECT_FALSE(s.is_updated());

    s.set_reset_each_iter();
    EXPECT_TRUE(s.need_reset_each_iter());
    s.set_keep_aggregate();
    EXPECT_FALSE(s.need_reset_each_iter());

    s.set_active();
    EXPECT_TRUE(s.is_active());
    s.set_inactive();
    EXPECT_FALSE(s.is_active());

    s.set_removed();
    EXPECT_TRUE(s.is_removed());
    s.set_non_removed();
    EXPECT_FALSE(s.is_removed());

    SomeAggregatorState s_;
    s.set_non_zero();
    s_.set_zero();
    s.set_updated();
    s_.set_non_updated();
    s.set_keep_aggregate();
    s_.set_reset_each_iter();
    s.set_inactive();
    s_.set_active();
    s.set_non_removed();
    s_.set_removed();
    EXPECT_NE(s.is_zero(), s_.is_zero());
    EXPECT_NE(s.is_updated(), s_.is_updated());
    EXPECT_NE(s.need_reset_each_iter(), s_.need_reset_each_iter());
    EXPECT_NE(s.is_active(), s_.is_active());
    EXPECT_NE(s.is_removed(), s_.is_removed());

    s.sync_option(s_);
    EXPECT_NE(s.is_zero(), s_.is_zero());
    EXPECT_NE(s.is_updated(), s_.is_updated());
    EXPECT_EQ(s.need_reset_each_iter(), s_.need_reset_each_iter());
    EXPECT_EQ(s.is_active(), s_.is_active());
    EXPECT_EQ(s.is_removed(), s_.is_removed());
}

TEST_F(TestAggregatorObject, Basic) {
    auto sum = [](int& a, int b) { a += b; };
    auto zero = [](int& a) { a = 0; };
    auto load = [](husky::base::BinStream& bin, int& a) { bin >> a; };
    auto save = [](husky::base::BinStream& bin, int a) { bin << a; };
    husky::lib::AggregatorObject<int> agg(sum, zero, load, save);

    int val = 2;
    agg.zero(val);
    EXPECT_EQ(val, 0);

    agg.set_non_zero();

    husky::base::BinStream bin;
    bin << 4;
    agg.load(bin);
    EXPECT_EQ(agg.get_value(), 4);

    agg.aggregate(8);
    EXPECT_EQ(agg.get_value(), 4 + 8);

    agg.aggregate([](int& a) { a += 16; });
    EXPECT_EQ(agg.get_value(), 4 + 8 + 16);

    husky::base::BinStream save_bin;
    agg.save(save_bin);
    save_bin >> val;
    EXPECT_EQ(val, 4 + 8 + 16);
}
