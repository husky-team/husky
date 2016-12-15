#include "core/shuffle_combiner.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <utility>
#include <vector>

#include "boost/random.hpp"
#include "gtest/gtest.h"

#include "core/zmq_helpers.hpp"

namespace husky {

namespace {

class TestShuffleCombiner : public testing::Test {
   public:
    TestShuffleCombiner() {}
    ~TestShuffleCombiner() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestShuffleCombiner, ShuffleCombiner) {
    const int num_global_workers = 4, num_local_workers = 2, round = 3, max_time = 10;
    int total_sum = 0;
    for (int i = 0; i < num_local_workers; i++)
        total_sum += i;
    zmq::context_t context(1);
    std::atomic_int done(0);
    std::vector<ShuffleCombiner<std::pair<int, int>>> workers(num_local_workers);
    std::vector<std::thread*> threads(num_local_workers);
    ShuffleCombiner<std::pair<int, int>>::init_sockets(num_local_workers, 0, context);
    for (int i = 0; i < num_local_workers; i++) {
        workers[i].init(num_global_workers, num_local_workers, 0, i);
    }
    for (int i = 0; i < num_local_workers; i++) {
        threads[i] = new std::thread([&workers, i, max_time, &done, total_sum]() {
            boost::random::mt19937 gen;
            boost::random::uniform_int_distribution<> random{1, max_time};
            int sum = 0;
            std::this_thread::sleep_for(std::chrono::milliseconds(random(gen)));
            workers[i].send_shuffler_buffer();

            for (int j = 0; j < num_local_workers - 1; j++) {
                int worker_id = workers[i].access_next();
                sum += worker_id;
            }
            EXPECT_EQ(sum, total_sum - i);
            done++;
        });
    }
    std::thread([&done, num_local_workers]() {
        auto max_wait = std::chrono::seconds(5);
        for (auto start = std::chrono::system_clock::now();
             done.load() != num_local_workers && std::chrono::system_clock::now() - start < max_wait;)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        EXPECT_EQ(done.load(), num_local_workers);
    }).join();
    for (auto i : threads) {
        i->join();
        delete i;
    }
    ShuffleCombiner<std::pair<int, int>>::erase_key(0);
}

}  // namespace

}  // namespace husky
