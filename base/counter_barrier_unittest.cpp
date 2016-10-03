#include "base/counter_barrier.hpp"

#include <chrono>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace husky {
namespace base {

class TestCounterBarrier : public testing::Test {
   public:
    TestCounterBarrier() {}
    ~TestCounterBarrier() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class TestFutureCounterBarrier : public testing::Test {
   public:
    TestFutureCounterBarrier() {}
    ~TestFutureCounterBarrier() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

void timer(std::function<void()> exec, int time_millis) {
    bool end = false;
    std::thread t([&end, exec]() {
        exec();
        end = true;
    });
    std::thread([&end, time_millis]() {
        auto max_wait = std::chrono::milliseconds(time_millis);
        for (auto start = std::chrono::system_clock::now(); !end && std::chrono::system_clock::now() - start < max_wait;
             std::this_thread::sleep_for(std::chrono::milliseconds(1))) {
        }
        EXPECT_TRUE(end);
    }).join();
    t.join();
}

template <typename BarrierType>
void test_single_unwait() {
    BarrierType lock;
    lock.set_target_count(1);
    timer([&]() { lock.lock(); }, 1000);
}

template <typename BarrierType>
void test_single_wait() {
    BarrierType lock;
    lock.set_target_count(1);
    timer([&]() { lock.lock(true); }, 1000);
}

template <typename BarrierType>
void test_partial_wait(int total, int wait = 0) {
    ASSERT_GE(wait, 0);
    ASSERT_GT(total, 0);
    ASSERT_GE(total, wait);
    BarrierType lock;
    lock.set_target_count(total + 1);
    std::vector<std::thread*> t(total);
    for (int i = 0; i < total; i++) {
        t[i] = new std::thread([&]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            lock.lock(i < wait);
        });
    }
    timer([&]() { lock.lock(true); }, 1000);
    for (int i = 0; i < total; i++) {
        t[i]->join();
        delete t[i];
    }
}

TEST_F(TestCounterBarrier, single_wait) { test_single_wait<CounterBarrier>(); }

TEST_F(TestCounterBarrier, single_unwait) { test_single_unwait<CounterBarrier>(); }

TEST_F(TestCounterBarrier, no_wait) { test_partial_wait<CounterBarrier>(100, 0); }

TEST_F(TestCounterBarrier, partial_wait_small) { test_partial_wait<CounterBarrier>(100, 10); }

TEST_F(TestCounterBarrier, partial_wait) { test_partial_wait<CounterBarrier>(100, 50); }

TEST_F(TestCounterBarrier, full_wait) { test_partial_wait<CounterBarrier>(100, 100); }

TEST_F(TestFutureCounterBarrier, single_wait) { test_single_wait<FutureCounterBarrier>(); }

TEST_F(TestFutureCounterBarrier, single_unwait) { test_single_unwait<FutureCounterBarrier>(); }

TEST_F(TestFutureCounterBarrier, no_wait) { test_partial_wait<FutureCounterBarrier>(100, 0); }

TEST_F(TestFutureCounterBarrier, partial_wait_small) { test_partial_wait<FutureCounterBarrier>(100, 10); }

TEST_F(TestFutureCounterBarrier, partial_wait) { test_partial_wait<FutureCounterBarrier>(100, 50); }

TEST_F(TestFutureCounterBarrier, full_wait) { test_partial_wait<FutureCounterBarrier>(100, 100); }

}  // namespace base
}  // namespace husky
