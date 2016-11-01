#include "core/shuffler.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace husky {
namespace {

class TestShuffler : public testing::Test {
   public:
    TestShuffler() {}
    ~TestShuffler() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestShuffler, Shuffler) {
    const int N = 4, Round = 2, MaxTime = 10;
    unsigned int seed = time(NULL);
    std::vector<Shuffler<int>> V(N);
    std::vector<std::thread*> workers(N);
    std::atomic_int done(0);
    for (auto& i : V)
        i.init(N);
    for (int i = 0; i < N; i++) {
        workers[i] = new std::thread([&V, i, N, Round, MaxTime, &done, &seed]() {
            Shuffler<int>& v = V[i];
            for (int round = 1, sum = 0; round <= Round; round++, sum = 0) {
                v.storage() += i * round;
                std::this_thread::sleep_for(std::chrono::milliseconds(rand_r(&seed) % MaxTime));
                v.commit();
                for (int j = 0; j < N; j++) {
                    sum += V[j].access();
                    std::this_thread::sleep_for(std::chrono::milliseconds(rand_r(&seed) % MaxTime));
                    V[j].leave();
                }
                EXPECT_EQ(sum << 1, N * (N - 1) * round);
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

TEST_F(TestShuffler, ShuffleCombiner) {
    const int N = 2, Round = 3, MaxTime = 10;
    unsigned int seed = time(NULL);
    std::atomic_int done(0);
    std::vector<ShuffleCombiner<std::pair<int, int>>> V(N);
    std::vector<std::thread*> workers(N);
    for (auto& i : V)
        i.init(N);
    for (int i = 0; i < N; i++) {
        workers[i] = new std::thread([&V, i, N, Round, MaxTime, &done, &seed]() {
            auto& v = V[i];
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
                EXPECT_EQ(sum, round * N * N * (N * i + N - 1));
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
