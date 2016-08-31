#include "core/shuffler.hpp"

#include <stdlib.h>
#include <time.h>

#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "core/context.hpp"

namespace husky {

class TestShuffler : public testing::Test {
   public:
    TestShuffler() {}
    ~TestShuffler() {}

   protected:
    void SetUp() { Context::init_global(); }
    void TearDown() { Context::finalize_global(); }
};

TEST_F(TestShuffler, Shuffler) {
    const int N = 4, Round = 2, MaxTime = 3;
    unsigned int seed = time(NULL);
    std::vector<Shuffler<int>> V;
    std::vector<std::thread*> workers(N);
    std::atomic_int done(0);
    for (int i = 0; i < N; i++)
        V.push_back(Shuffler<int>("Shuffler", N));
    for (int i = 0; i < N; i++) {
        workers[i] = new std::thread([&V, i, N, Round, MaxTime, &done, &seed]() {
            Context::init_local();
            Shuffler<int>& v = V[i];
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
                ASSERT_EQ(sum << 1, N * (N - 1) * round);
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

TEST_F(TestShuffler, ShuffleCombiner) {
    const int N = 2, Round = 3, MaxTime = 4;
    unsigned int seed = time(NULL);
    std::atomic_int done(0);
    std::vector<ShuffleCombiner<std::pair<int, int>>> V;
    std::vector<std::thread*> workers(N);
    for (int i = 0; i < N; i++)
        V.push_back(std::move(ShuffleCombiner<std::pair<int, int>>("ShuffleCombiner", N)));
    for (int i = 0; i < N; i++) {
        workers[i] = new std::thread([&V, i, N, Round, MaxTime, &done, &seed]() {
            Context::init_local();
            auto& v = V[i];
            v.init();
            for (int round = 1, sum = 0; round <= Round; round++, sum = 0) {
                for (int k = 0; k < N; k++) {
                    for (int m = 0; m < N; m++)
                        v.storage(k).push_back(std::make_pair((k * N + i + m) * round, 1));
                    std::this_thread::sleep_for(std::chrono::milliseconds(rand_r(&seed) % MaxTime));
                    v.commit(k);
                }
                std::vector<std::pair<int, int>> tmp;
                for (int j = 0; j < N; j++) {
                    tmp.insert(tmp.end(), V[j].access(i).begin(), V[j].access(i).end());
                    std::this_thread::sleep_for(std::chrono::milliseconds(rand_r(&seed) % MaxTime));
                    V[j].leave(i);
                }
                for (auto& x : tmp)
                    sum += x.first * x.second;
                ASSERT_EQ(sum, round * N * N * (N * i + N - 1));
            }
            done++;
        });
    }
    std::thread([&done, MaxTime, Round, N]() {
        auto MaxWait = std::chrono::milliseconds(MaxTime * Round * N * N * 200);
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

}  // namespace husky
