#include "core/accessor.hpp"

#include <stdlib.h>
#include <time.h>

#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "core/context.hpp"

namespace {

using namespace husky;

class TestAccessor : public testing::Test {
   public:
    TestAccessor() {}
    ~TestAccessor() {}

   protected:
    void SetUp() { Context::init_global(); }
    void TearDown() { Context::finalize_global(); }
};

TEST_F(TestAccessor, Accessor) {
    const int N = 4, Round = 3, MaxTime = 2;
    std::atomic_int done(0);
    std::vector<Accessor<int>> V;
    std::vector<std::thread*> workers(N);
    for (int i = 0; i < N; i++)
        V.push_back(Accessor<int>("Accessor", N));
    for (int i = 0; i < N; i++) {
        workers[i] = new std::thread([&V, i, N, Round, MaxTime, &done]() {
            unsigned int seed = time(NULL);
            Context::init_local();
            Accessor<int>& v = V[i];
            v.init();
            for (int round = 1, sum = 0; round <= Round; round++, sum = 0) {
                v.storage() += i * round;
                std::this_thread::sleep_for(std::chrono::milliseconds(rand_r(&seed) % MaxTime));
                v.commit();
                for (int j = 0; j < N; j++) {
                    sum += V[j].access();
                    std::this_thread::sleep_for(std::chrono::milliseconds(rand_r(&seed) % MaxTime));
                    V[j].leave();
                }
                ASSERT_EQ(sum << 2, N * (N - 1) * round * (round + 1));
            }
            done++;
        });
    }
    std::thread([&done, MaxTime, Round, N]() {
        auto MaxWait = std::chrono::milliseconds(MaxTime * Round * N * 200);
        for (auto start = std::chrono::system_clock::now();
             done.load() != N && std::chrono::system_clock::now() - start < MaxWait;)
            std::this_thread::sleep_for(std::chrono::milliseconds(MaxTime));
        ASSERT_EQ(done.load(), N);
    }).join();
    for (auto i : workers) {
        i->join();
        delete i;
    }
}

}  // namespace
