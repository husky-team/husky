#include "core/accessor.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace husky {
namespace {

class TestAccessor : public testing::Test {
   public:
    TestAccessor() {}
    ~TestAccessor() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestAccessor, InitAndDelete) {
    Accessor<int>* accessor = new Accessor<int>();
    ASSERT_TRUE(accessor != nullptr);
    delete accessor;
}

TEST_F(TestAccessor, Functional) {
    Accessor<int>* accessor = new Accessor<int>();
    accessor->init(1);
    ASSERT_EQ(accessor->storage(), 0);
    accessor->commit();
    ASSERT_EQ(accessor->access(), 0);
    accessor->leave();
    int num = 10;
    accessor->commit(num);
    ASSERT_EQ(accessor->access(), num);
    accessor->leave();
    ASSERT_EQ(accessor->storage(), num);
    delete accessor;
}

TEST_F(TestAccessor, FunctionalInMultiThreads) {
    const int N = 3, Round = 3, MaxTime = 3;
    std::atomic_int done(0);
    std::vector<Accessor<int>> V(N);
    std::vector<std::thread*> workers(N);
    for (auto& i : V)
        i.init(N);
    for (int i = 0; i < N; i++) {
        workers[i] = new std::thread([&V, i, N, Round, MaxTime, &done]() {
            srand(time(NULL));
            Accessor<int>& v = V[i];
            for (int round = 1, sum = 0; round <= Round; round++, sum = 0) {
                v.storage() += i * round;
                std::this_thread::sleep_for(std::chrono::milliseconds(rand() % MaxTime));
                v.commit();
                for (int j = 0; j < N; j++) {
                    sum += V[j].access();
                    std::this_thread::sleep_for(std::chrono::milliseconds(rand() % MaxTime));
                    V[j].leave();
                }
                EXPECT_EQ(sum << 2, N * (N - 1) * round * (round + 1));
            }
            done++;
        });
    }
    std::thread([&done, N]() {
        auto max_wait = std::chrono::seconds(5);
        for (auto start = std::chrono::system_clock::now();
             done.load() != N && std::chrono::system_clock::now() - start < max_wait;)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        EXPECT_EQ(done.load(), N);
    }).join();
    for (auto i : workers) {
        i->join();
        delete i;
    }
}

}  // namespace
}  // namespace husky
