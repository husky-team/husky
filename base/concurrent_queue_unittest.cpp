#include "base/concurrent_queue.hpp"

#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "base/serialization.hpp"

namespace husky {
namespace {

using base::BinStream;
using base::ConcurrentQueue;

class TestConcurrentQueue : public testing::Test {
   public:
    TestConcurrentQueue() {}
    ~TestConcurrentQueue() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestConcurrentQueue, InitAndDelete) {
    ConcurrentQueue<bool>* queue_bool = new ConcurrentQueue<bool>();
    ASSERT_TRUE(queue_bool != NULL);
    ConcurrentQueue<double>* queue_double = new ConcurrentQueue<double>();
    ASSERT_TRUE(queue_double != NULL);
    ConcurrentQueue<int>* queue_int = new ConcurrentQueue<int>();
    ASSERT_TRUE(queue_int != NULL);
    ConcurrentQueue<std::vector<int>>* queue_vector_int = new ConcurrentQueue<std::vector<int>>();
    ASSERT_TRUE(queue_vector_int != NULL);
    ConcurrentQueue<BinStream>* queue_bin_stream = new ConcurrentQueue<BinStream>();
    ASSERT_TRUE(queue_vector_int != NULL);
    delete queue_bool;
    delete queue_double;
    delete queue_int;
    delete queue_vector_int;
    delete queue_bin_stream;
}

TEST_F(TestConcurrentQueue, CopyConstructor) {
    ConcurrentQueue<int> queue;
    ASSERT_TRUE(queue.is_empty());

    std::vector<int> data = {1, 3, -6, 0};
    for (int i = 0; i < data.size(); ++i)
        queue.push(std::move(data[i]));

    ConcurrentQueue<int> queue_copy(std::move(queue));
    ASSERT_TRUE(queue.is_empty());
    EXPECT_EQ(queue_copy.size(), 4);
    for (int i = 0; i < data.size(); ++i)
        EXPECT_EQ(queue_copy.pop(), data[i]);
    ASSERT_TRUE(queue_copy.is_empty());
}

TEST_F(TestConcurrentQueue, EqualOperation) {
    ConcurrentQueue<int> queue;
    ASSERT_TRUE(queue.is_empty());

    std::vector<int> data = {1, 3, -6, 0};
    for (int i = 0; i < data.size(); ++i)
        queue.push(std::move(data[i]));
    EXPECT_EQ(queue.size(), 4);

    ConcurrentQueue<int> queue_copy = std::move(queue);
    ASSERT_TRUE(queue.is_empty());
    EXPECT_EQ(queue_copy.size(), 4);
    for (int i = 0; i < data.size(); ++i)
        EXPECT_EQ(queue_copy.pop(), data[i]);
    ASSERT_TRUE(queue_copy.is_empty());
}

TEST_F(TestConcurrentQueue, PushAndPop) {
    ConcurrentQueue<int> queue;
    ASSERT_TRUE(queue.is_empty());
    std::vector<int> data = {1, 3, -6, 0};

    for (int i = 0; i < data.size(); ++i)
        queue.push(std::move(data[i]));
    EXPECT_EQ(queue.size(), 4);

    for (int i = 0; i < data.size(); ++i)
        EXPECT_EQ(queue.pop(), data[i]);
    ASSERT_TRUE(queue.is_empty());
}

TEST_F(TestConcurrentQueue, ConcurrentPushAndPop) {
    ConcurrentQueue<int> queue;
    int num_thread = 3;
    int push_size = 10;
    int pop_size = 5;
    std::vector<std::thread*> threads(num_thread, nullptr);
    for (int i = 0; i < num_thread; i++) {
        threads[i] = new std::thread([&queue]() {
            for (int j = 0; j < 10; j++)
                queue.push(std::move(j));
            for (int k = 0; k < 5; k++) {
                int pop = queue.pop();
                EXPECT_TRUE(pop >= 0 && pop < 10);
            }
        });
    }

    for (int i = 0; i < num_thread; i++) {
        threads[i]->join();
        delete threads[i];
        threads[i] = nullptr;
    }

    // queue.size() = num_thread * (push_size - pop_size)
    EXPECT_EQ(queue.size(), num_thread * 5);
}

}  // namespace
}  // namespace husky
