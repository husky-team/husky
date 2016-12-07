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

    for (int i = 1; i <= procs.size(); i++) {
        worker_info.set_hostname(i, procs[i - 1]);
        for (int j = 0; j < threads; j++) {
            worker_info.add_worker(i, num_threads, j);
            num_threads++;
        }
    }

    worker_info.set_process_id(3);

    EXPECT_EQ(worker_info.get_process_id(), 3);
    EXPECT_EQ(worker_info.get_num_workers(), num_threads);
    EXPECT_EQ(worker_info.get_num_processes(), procs.size());

    EXPECT_EQ(worker_info.get_num_local_workers(), threads);
    EXPECT_EQ(worker_info.get_num_local_workers(2), threads);

    EXPECT_EQ(worker_info.get_hostname(1), "aa");
    EXPECT_EQ(worker_info.get_hostname(2), "bb");
    EXPECT_EQ(worker_info.get_hostname(3), "dd");

    EXPECT_EQ(worker_info.get_process_id(1), 1);
    EXPECT_EQ(worker_info.get_process_id(21), 2);
    EXPECT_EQ(worker_info.get_process_id(40), 3);
    EXPECT_EQ(worker_info.get_process_id(50), 3);

    EXPECT_EQ(worker_info.local_to_global_id(2, 1), 21);
}

}  // namespace
}  // namespace husky
