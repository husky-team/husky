#include "lib/aggregator.hpp"

#include <algorithm>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "base/concurrent_queue.hpp"
#include "base/serialization.hpp"
#include "base/thread_support.hpp"
#include "core/accessor.hpp"
#include "lib/aggregator_object.hpp"

#include "gtest/gtest.h"

namespace husky {

class NonEmptyCtor {
   public:
    int some_mem;
    NonEmptyCtor() = default;
    explicit NonEmptyCtor(int some_int) : some_mem(some_int) {}
};

void some_func(bool& a, bool b) { a &= b; }

namespace lib {
template <>
void copy_assign<std::shared_ptr<NonEmptyCtor>>(std::shared_ptr<NonEmptyCtor>& a,
                                                const std::shared_ptr<NonEmptyCtor>& b) {
    // instead of `a = b;`
    a = std::make_shared<NonEmptyCtor>(b->some_mem);
}

template <>
void to_stream<std::shared_ptr<NonEmptyCtor>>(std::ostream& os, const std::shared_ptr<NonEmptyCtor>& b) {
    os << b->some_mem;
}
}  // namespace lib

namespace {

using base::BinStream;
using base::ConcurrentQueue;
using base::CallOnceEachTime;
using base::KBarrier;
using lib::Aggregator;
using lib::AggregatorFactoryBase;
using lib::AggregatorState;

class MultiMachineAggregatorFactory : public AggregatorFactoryBase {
   protected:
    virtual size_t get_factory_id() { return fid; }

    virtual size_t get_num_global_factory() { return info_size_.back(); }

    virtual size_t get_num_local_factory() { return factory_info_[mid]; }

    virtual size_t get_num_machine() { return factory_info_.size(); }

    virtual size_t get_machine_id() { return mid; }

    virtual size_t get_machine_id(size_t fid) {
        return std::upper_bound(info_size_.begin(), info_size_.end(), fid) - info_size_.begin();
    }

    void send(ConcurrentQueue<BinStream>* queue, std::vector<BinStream>& bin) {
        for (int i = 0, sz = info_size_.back(); i < sz; ++i) {
            queue[i].push(std::move(bin[i]));
        }
    }

    void on_recv(ConcurrentQueue<BinStream>* queue, const std::function<void(BinStream&)>& recv) {
        ConcurrentQueue<BinStream>& bin = queue[fid];
        for (int i = 0, sz = info_size_.back(); i < sz; ++i) {
            while (bin.is_empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            BinStream b = bin.pop();
            if (b.size() != 0) {
                recv(b);
            }
        }
    }

    virtual void send_local_update(std::vector<BinStream>& bins) { send(aggregate_queues_, bins); }

    virtual void on_recv_local_update(const std::function<void(BinStream&)>& recv) { on_recv(aggregate_queues_, recv); }

    virtual void broadcast_aggregator(std::vector<BinStream>& bins) { send(broadcast_queues_, bins); }

    virtual void on_recv_broadcast(const std::function<void(BinStream&)>& recv) { on_recv(broadcast_queues_, recv); }

    class SharedData : public InnerSharedData {
       public:
        KBarrier barrier_;
        std::vector<Accessor<std::vector<AggregatorState*>>*> all_local_agg_accessor_;
        virtual void initialize(AggregatorFactoryBase* factory) {
            InnerSharedData::initialize(factory);
            size_t num_local_factory = static_cast<MultiMachineAggregatorFactory*>(factory)->get_num_local_factory();
            all_local_agg_accessor_.resize(num_local_factory);
            for (auto& accessor : all_local_agg_accessor_) {
                accessor = new Accessor<std::vector<AggregatorState*>>();
                accessor->init(num_local_factory);
            }
        }
        virtual ~SharedData() {
            for (auto accessor : all_local_agg_accessor_) {
                delete accessor;
            }
        }
    } * shared_{nullptr};

    virtual void on_access_other_local_update(const std::function<void(std::vector<AggregatorState*>&)>& update) {
        shared_->all_local_agg_accessor_[get_local_factory_idx()]->commit(get_local_aggs());
        for (size_t i = 0, sz = get_num_local_factory(); i < sz; ++i) {
            Accessor<std::vector<AggregatorState*>>* a = shared_->all_local_agg_accessor_[i];
            update(a->access());
            a->leave();
        }
    }

    virtual InnerSharedData* create_shared_data() { return new SharedData(); }

    virtual void wait_for_others() { shared_->barrier_.wait(get_num_local_factory()); }

    virtual void call_once(const std::function<void()>& lambda) { call_onces_[get_machine_id()](lambda); }

    virtual void init_factory() {
        AggregatorFactoryBase::init_factory();
        shared_ = static_cast<SharedData*>(get_shared_data());
    }

    virtual void set_factory_leader(AggregatorFactoryBase* leader) { leaders_[mid] = leader; }

    virtual AggregatorFactoryBase* get_factory_leader() { return leaders_[mid]; }

    static std::vector<int> info_size_;
    static std::vector<AggregatorFactoryBase*> leaders_;
    static std::vector<int> factory_info_;
    static ConcurrentQueue<BinStream>* aggregate_queues_;
    static ConcurrentQueue<BinStream>* broadcast_queues_;
    static CallOnceEachTime* call_onces_;

   public:
    static void initialize(const std::vector<int>& info) {
        factory_info_ = info;
        info_size_.resize(factory_info_.size());
        for (int i = 0; i < info_size_.size(); ++i) {
            info_size_[i] = factory_info_[i];
            if (i != 0) {
                info_size_[i] += info_size_[i - 1];
            }
        }
        leaders_.resize(factory_info_.size(), nullptr);
        call_onces_ = new CallOnceEachTime[info_size_.back()];
        aggregate_queues_ = new ConcurrentQueue<BinStream>[info_size_.back()];
        broadcast_queues_ = new ConcurrentQueue<BinStream>[info_size_.back()];
    }

    static void finalize() {
        factory_info_.clear();
        leaders_.clear();
        delete[] call_onces_;
        delete[] aggregate_queues_;
        delete[] broadcast_queues_;
    }

    static thread_local int fid;
    static thread_local int mid;
};

std::vector<int> MultiMachineAggregatorFactory::factory_info_;
std::vector<int> MultiMachineAggregatorFactory::info_size_;
std::vector<AggregatorFactoryBase*> MultiMachineAggregatorFactory::leaders_;
CallOnceEachTime* MultiMachineAggregatorFactory::call_onces_;
ConcurrentQueue<BinStream>* MultiMachineAggregatorFactory::broadcast_queues_;
ConcurrentQueue<BinStream>* MultiMachineAggregatorFactory::aggregate_queues_;
thread_local int MultiMachineAggregatorFactory::fid;
thread_local int MultiMachineAggregatorFactory::mid;

void test_aggregator(int fid, int mid, int num_global_factory) {
    // different blocks are to test removing aggregators
    MultiMachineAggregatorFactory::fid = fid;
    MultiMachineAggregatorFactory::mid = mid;
    EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 0);
    {
        for (int i = 1; i <= 5; ++i) {
            Aggregator<int> a;
            a.update(1);
            AggregatorFactoryBase::sync();
            EXPECT_EQ(a.get_value(), 1 * num_global_factory);
        }
        Aggregator<int> a;
        for (int i = 1; i <= 5; ++i) {
            a.update(1);
            AggregatorFactoryBase::sync();
            EXPECT_EQ(a.get_value(), i * num_global_factory);
        }
    }
    EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 0);
    {
        for (int i = 1; i <= 5; ++i) {
            Aggregator<int> a, b, c;
            a.update(1);
            b.update(1);
            c.update(1);
            AggregatorFactoryBase::sync();
            EXPECT_EQ(a.get_value(), 1 * num_global_factory);
            EXPECT_EQ(b.get_value(), 1 * num_global_factory);
            EXPECT_EQ(c.get_value(), 1 * num_global_factory);
        }
        Aggregator<int> a, b, c;
        for (int i = 1; i <= 5; ++i) {
            a.update(1);
            b.update(1);
            c.update(1);
            AggregatorFactoryBase::sync();
            EXPECT_EQ(a.get_value(), i * num_global_factory);
            EXPECT_EQ(b.get_value(), i * num_global_factory);
            EXPECT_EQ(c.get_value(), i * num_global_factory);
        }
    }
    EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 0);
    {
        EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 0);
        Aggregator<int> a;
        a.to_reset_each_iter();
        EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 1);
        Aggregator<bool> b(true, some_func);
        EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 2);
        Aggregator<std::shared_ptr<NonEmptyCtor>> c(
            std::make_shared<NonEmptyCtor>(1),
            [](std::shared_ptr<NonEmptyCtor> a, std::shared_ptr<NonEmptyCtor> b) { a->some_mem += b->some_mem; },
            [](std::shared_ptr<NonEmptyCtor>& x) { x = std::make_shared<NonEmptyCtor>(1); });
        c.to_reset_each_iter();
        EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 3);
        Aggregator<double> d;  // test default keep_aggregate
        EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 4);
        EXPECT_EQ(a.get_value(), 0);  // not updated
        EXPECT_EQ(b.get_value(), true);
        ASSERT_NE(c.get_value(), nullptr);
        EXPECT_EQ(c.get_value()->some_mem, 1);
        EXPECT_EQ(d.get_value(), 0.);
        a.update(1);
        b.update_any([](bool& x) { x = false; });
        c.update(std::make_shared<NonEmptyCtor>(2));
        d.update(fid);
        EXPECT_EQ(a.get_value(), 0);  // not synced
        EXPECT_EQ(b.get_value(), true);
        ASSERT_NE(c.get_value(), nullptr);
        EXPECT_EQ(c.get_value()->some_mem, 1);
        EXPECT_EQ(d.get_value(), 0.);
        AggregatorFactoryBase::sync();
        EXPECT_EQ(a.get_value(), 1 * num_global_factory);
        EXPECT_EQ(b.get_value(), false);
        ASSERT_NE(c.get_value(), nullptr);
        EXPECT_EQ(c.get_value()->some_mem, 1 + 3 * num_global_factory);
        EXPECT_EQ(d.get_value(), (num_global_factory - 1) / 2. * num_global_factory);
        a.update(2);
        d.update(fid);
        EXPECT_EQ(a.get_value(), 1 * num_global_factory);
        EXPECT_EQ(d.get_value(), (num_global_factory - 1) / 2. * num_global_factory);
        AggregatorFactoryBase::sync();
        EXPECT_EQ(a.get_value(), 2 * num_global_factory);  // NOT 3 * num_global_factory
        EXPECT_EQ(b.get_value(), false);                   // Back to init value
        ASSERT_NE(c.get_value(), nullptr);
        EXPECT_EQ(c.get_value()->some_mem, 1);  // Back to init value
        EXPECT_EQ(d.get_value(), (num_global_factory - 1) * num_global_factory);
    }
    EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 0);
    {
        Aggregator<int> a, b, c;
        EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 3);
        a.update(1);
        b.update(1);
        c.update(1);
        AggregatorFactoryBase::sync();
        EXPECT_EQ(a.get_value(), 1 * num_global_factory);
        EXPECT_EQ(b.get_value(), 1 * num_global_factory);
        EXPECT_EQ(c.get_value(), 1 * num_global_factory);
        b.inactivate();
        a.update(1);
        b.update(1);
        c.update(1);
        AggregatorFactoryBase::sync();
        EXPECT_EQ(a.get_value(), 2 * num_global_factory);
        EXPECT_EQ(b.get_value(), 1 * num_global_factory);
        EXPECT_EQ(c.get_value(), 2 * num_global_factory);
        c.inactivate();
        a.update(1);
        b.update(1);
        c.update(1);
        AggregatorFactoryBase::sync();
        EXPECT_EQ(a.get_value(), 3 * num_global_factory);
        EXPECT_EQ(b.get_value(), 1 * num_global_factory);
        EXPECT_EQ(c.get_value(), 2 * num_global_factory);
        AggregatorFactoryBase::inactivate();
        AggregatorFactoryBase::sync();
        EXPECT_EQ(a.get_value(), 3 * num_global_factory);
        EXPECT_EQ(b.get_value(), 1 * num_global_factory);
        EXPECT_EQ(c.get_value(), 2 * num_global_factory);
        a.update(1);
        b.update(1);
        c.update(1);
        AggregatorFactoryBase::activate();
        b.activate();
        c.activate();
        AggregatorFactoryBase::sync();
        EXPECT_EQ(a.get_value(), 4 * num_global_factory);
        EXPECT_EQ(b.get_value(), 4 * num_global_factory);
        EXPECT_EQ(c.get_value(), 4 * num_global_factory);
    }
    EXPECT_EQ(AggregatorFactoryBase::get_num_aggregator(), 0);
}

class WorkerNodeFactory {
   private:
    std::vector<std::vector<std::thread*>> threads_;
    std::vector<int> info_;

   public:
    explicit WorkerNodeFactory(const std::vector<int>& node_info) : info_(node_info) {}
    WorkerNodeFactory& start(std::function<void(int, int, int)> job) {
        int fid = 0;
        int num_global_factory = 0;
        for (auto i : info_) {
            num_global_factory += i;
        }
        threads_.resize(info_.size());
        MultiMachineAggregatorFactory::initialize(info_);
        for (int i = 0; i < info_.size(); i++) {
            threads_[i].resize(info_[i]);
            for (int j = 0; j < info_[i]; j++) {
                threads_[i][j] = new std::thread(std::bind(job, fid++, i, num_global_factory));
            }
        }
        return *this;
    }
    void join() {
        for (int i = 0; i < info_.size(); i++) {
            for (int j = 0; j < info_[i]; j++) {
                threads_[i][j]->join();
                delete threads_[i][j];
            }
            threads_[i].clear();
        }
        MultiMachineAggregatorFactory::finalize();
    }
};

class TestAggregator : public testing::Test {
   public:
    TestAggregator() {}
    ~TestAggregator() {}

   protected:
    void SetUp() {
        // register the factory class here before all the tests
        AggregatorFactoryBase::register_factory_constructor([]() { return new MultiMachineAggregatorFactory(); });
    }
    void TearDown() {}
};

TEST_F(TestAggregator, SingleMachineSingleThread) { WorkerNodeFactory({1}).start(test_aggregator).join(); }

TEST_F(TestAggregator, SingleMachineMultiThread_small) { WorkerNodeFactory({2}).start(test_aggregator).join(); }

TEST_F(TestAggregator, SingleMachineMultiThread_large) { WorkerNodeFactory({40}).start(test_aggregator).join(); }

TEST_F(TestAggregator, MultiMachineSingleThread) { WorkerNodeFactory({1, 1}).start(test_aggregator).join(); }

TEST_F(TestAggregator, MultiMachineMultiThread_small) { WorkerNodeFactory({2, 2}).start(test_aggregator).join(); }

TEST_F(TestAggregator, MultiMachineSingleThread_more) {
    WorkerNodeFactory({1, 1, 1, 1, 1, 1, 1, 1, 1, 1}).start(test_aggregator).join();
}

TEST_F(TestAggregator, MultiMachineMultiThread_medium) { WorkerNodeFactory({20, 20}).start(test_aggregator).join(); }

TEST_F(TestAggregator, MultiMachineMultiThread_large) {
    WorkerNodeFactory({10, 10, 10, 10}).start(test_aggregator).join();
}

TEST_F(TestAggregator, MultiMachineMultiThread_plenty) {
    WorkerNodeFactory({5, 5, 5, 5, 5, 5, 5, 5, 5, 5}).start(test_aggregator).join();
}

TEST_F(TestAggregator, MultiMachineMultiThread_any) {
    WorkerNodeFactory({1, 2, 3, 4, 5, 6, 7}).start(test_aggregator).join();
}

}  // namespace
}  // namespace husky
