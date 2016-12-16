#include "core/worker_info.hpp"

#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace husky {
namespace {

class TestWorkerInfo : public testing::Test {
   public:
    TestWorkerInfo() {}
    ~TestWorkerInfo() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestWorkerInfo, InitAndDelete) {
    WorkerInfo* worker_info = new WorkerInfo();
    ASSERT_TRUE(worker_info != NULL);
    delete worker_info;
}

TEST_F(TestWorkerInfo, Functional) {
    WorkerInfo worker_info;

    std::vector<std::string> procs;
    procs.push_back("aa");
    procs.push_back("bb");
    procs.push_back("dd");
    int threads = 20;
    int num_threads = 0;

    for (int i = 0; i < procs.size(); i++) {
        worker_info.set_hostname(i, procs[i]);
        for (int j = 0; j < threads; j++) {
            worker_info.add_worker(i, num_threads, j);
            num_threads++;
        }
    }

    worker_info.set_process_id(2);

    EXPECT_EQ(worker_info.get_process_id(), 2);
    EXPECT_EQ(worker_info.get_num_workers(), num_threads);
    EXPECT_EQ(worker_info.get_num_processes(), procs.size());

    EXPECT_EQ(worker_info.get_num_local_workers(), threads);
    EXPECT_EQ(worker_info.get_num_local_workers(2), threads);

    EXPECT_EQ(worker_info.get_hostname(0), "aa");
    EXPECT_EQ(worker_info.get_hostname(1), "bb");
    EXPECT_EQ(worker_info.get_hostname(2), "dd");

    EXPECT_EQ(worker_info.get_process_id(1), 0);
    EXPECT_EQ(worker_info.get_process_id(21), 1);
    EXPECT_EQ(worker_info.get_process_id(40), 2);
    EXPECT_EQ(worker_info.get_process_id(50), 2);

    EXPECT_EQ(worker_info.local_to_global_id(1, 1), 21);

    EXPECT_EQ(worker_info.get_largest_tid(), num_threads-1);
}

TEST_F(TestWorkerInfo, Inconsecutive) {
    WorkerInfo worker_info;
    // process_id, global_worker_id, local_worker_id
    worker_info.add_worker(0, 4, 0);
    worker_info.add_worker(0, 2, 2);
    worker_info.add_worker(0, 7, 4);
    worker_info.set_process_id(0);

    // get_largest_tid
    EXPECT_EQ(worker_info.get_largest_tid(), 7);

    // global_to_local_id
    EXPECT_EQ(worker_info.global_to_local_id(7), 4);
    EXPECT_EQ(worker_info.global_to_local_id(2), 2);
    EXPECT_EQ(worker_info.global_to_local_id(4), 0);

    // local_to_global_id
    EXPECT_EQ(worker_info.local_to_global_id(0), 4);
    EXPECT_EQ(worker_info.local_to_global_id(2), 2);
    EXPECT_EQ(worker_info.local_to_global_id(4), 7);

    // get_global_tids
    auto global_tids = worker_info.get_global_tids();
    std::sort(global_tids.begin(), global_tids.end());
    std::vector<int> global_tids_res{2, 4, 7};
    for (int i = 0; i < global_tids.size(); ++ i) {
        EXPECT_EQ(global_tids[i], global_tids_res[i]);
    }

    // get_pids
    auto pids = worker_info.get_pids();
    EXPECT_EQ(pids.size(), 1);
    EXPECT_EQ(pids[0], 0);

    // get_local_tids
    auto local_tids = worker_info.get_local_tids();
    std::sort(local_tids.begin(), local_tids.end());
    std::vector<int> local_tids_res{2, 4, 7};
    for (int i = 0; i < local_tids.size(); ++ i) {
        EXPECT_EQ(local_tids[i], local_tids_res[i]);
    }
}

}  // namespace
}  // namespace husky
