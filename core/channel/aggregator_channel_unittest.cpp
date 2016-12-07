#include "core/channel/aggregator_channel.hpp"

#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "base/serialization.hpp"
#include "core/hash_ring.hpp"
#include "core/mailbox.hpp"
#include "core/worker_info.hpp"

namespace husky {
namespace {

class TestAggregatorChannel : public testing::Test {
   public:
    TestAggregatorChannel() {}
    ~TestAggregatorChannel() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestAggregatorChannel, Create) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_process_id(0);

    // BroadcastChannel
    AggregatorChannel agg_channel;
    agg_channel.setup(0, 0, workerinfo, &mailbox);
}

TEST_F(TestAggregatorChannel, Aggregate) {
    // HashRing Setup
    HashRing hashring;
    hashring.insert(0, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // WorkerInfo Setup
    WorkerInfo workerinfo;
    workerinfo.add_worker(0, 0, 0);
    workerinfo.set_process_id(0);

    // BroadcastChannel
    AggregatorChannel agg_channel;
    agg_channel.setup(0, 0, workerinfo, &mailbox);

    std::vector<BinStream> bins(1);
    bins.front() << '1' << 2 << 3.f << 4.0 << 5ll << std::string("Hello World");
    agg_channel.send(bins);
    bins.clear();

    while (agg_channel.poll()) {
        BinStream bin = agg_channel.recv();
        char a;
        int b;
        float c;
        double d;
        int64_t e;
        std::string f;
        bin >> a >> b >> c >> d >> e >> f;
        EXPECT_EQ(a, '1');
        EXPECT_EQ(b, 2);
        EXPECT_EQ(c, 3.f);
        EXPECT_EQ(d, 4.0);
        EXPECT_EQ(e, 5ll);
        EXPECT_EQ(f, std::string("Hello World"));
    }
}

TEST_F(TestAggregatorChannel, MultiThread) {
    const int NUM_THREADS = 40;

    // HashRing Setup
    HashRing hashring;
    for (int i = 0; i < NUM_THREADS; i++)
        hashring.insert(i, 0);

    // Mailbox Setup
    zmq::context_t zmq_context;
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);

    std::vector<LocalMailbox*> mailboxes;
    for (int i = 0; i < NUM_THREADS; i++) {
        mailboxes.push_back(new LocalMailbox(&zmq_context));
        mailboxes[i]->set_thread_id(i);
        el.register_mailbox(*mailboxes[i]);
    }

    std::vector<std::thread*> threads;
    for (int i = 0; i < NUM_THREADS; i++) {
        threads.push_back(new std::thread([&, id = i, mailbox = mailboxes[i] ]() {
            // WorkerInfo Setup
            WorkerInfo workerinfo;
            for (int j = 0; j < NUM_THREADS; j++)
                workerinfo.add_worker(0, j, j);
            workerinfo.set_process_id(0);

            // BroadcastChannel
            AggregatorChannel agg_channel;
            agg_channel.setup(id, id, workerinfo, mailbox);

            std::vector<BinStream> bins(NUM_THREADS);
            for (int j = 0; j < NUM_THREADS; j++) {
                bins[j] << std::make_pair(id, j);
            }

            agg_channel.send(bins);
            bins.clear();

            std::vector<bool> marks(NUM_THREADS, false);
            std::pair<int, int> p;
            while (agg_channel.poll()) {
                BinStream bin = agg_channel.recv();
                while (bin.size() != 0) {
                    bin >> p;
                    EXPECT_EQ(p.second, id);
                    EXPECT_EQ(marks[p.first], false);
                    marks[p.first] = true;
                }
            }
            for (auto m : marks) {
                EXPECT_EQ(m, true);
            }
        }));
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        threads[i]->join();
        delete threads[i];
        delete mailboxes[i];
        threads[i] = nullptr;
        mailboxes[i] = nullptr;
    }
}

}  // namespace
}  // namespace husky
