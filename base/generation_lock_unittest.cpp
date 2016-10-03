#include "base/generation_lock.hpp"

#include <atomic>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace husky {
namespace base {

class TestGenerationLock : public testing::Test {
   public:
    TestGenerationLock() {}
    ~TestGenerationLock() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestGenerationLock, GenerationLock) {
    std::vector<std::thread*> t;
    const int N = 100;
    t.resize(N);
    GenerationLock lock;
    for (int i = 0; i < N; i++) {
        t[i] = new std::thread([i, N, &lock]() {
            for (int j = 0; j < N; j++) {
                if (j == i)
                    lock.notify();
                lock.wait();
            }
        });
    }
    for (auto& i : t) {
        i->join();
        delete i;
    }
}

TEST_F(TestGenerationLock, CallOnceEachTime) {
    std::vector<std::thread*> t;
    const int N = 100;
    t.resize(N);
    CallOnceEachTime call;
    std::atomic_int* num = new std::atomic_int[N];
    for (int i = 0; i < N; i++) {
        num[i].store(0);
    }
    for (int i = 0; i < N; i++) {
        t[i] = new std::thread([i, N, &num, &call]() {
            for (int j = 0; j < N; j++) {
                call([&]() { num[j]++; });
            }
        });
    }
    for (auto& i : t) {
        i->join();
        delete i;
    }
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(num[i].load(), 1);
    }
    delete[] num;
}

}  // namespace base
}  // namespace husky
