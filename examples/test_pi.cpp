#include <random>
#include <string>

#include "base/log.hpp"
#include "core/channel/channel_factory.hpp"
#include "core/context.hpp"
#include "core/executor.hpp"
#include "core/job_runner.hpp"
#include "core/objlist.hpp"

using namespace husky;

class Foo {
   public:
    typedef int KeyT;
    KeyT key;

    Foo(KeyT key) { this->key = key; }

    const KeyT& id() const { return key; }
};

void test_push_combined() {
    ObjList<Foo> obj_list;
    auto& ch = ChannelFactory::create_push_combined_channel<double, SumCombiner<double>>(obj_list, obj_list);
    double num_pts_inside = 0;
    int tot_pts = 1000;
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    for (int i = 0; i < tot_pts; i++) {
        double x = distribution(generator);
        double y = distribution(generator);
        if (x * x + y * y <= 1)
            num_pts_inside += 1;
    }
    double pi = num_pts_inside / tot_pts;
    ch.push(pi, 0);
    ch.flush();
    ch.prepare_messages();
    for (Foo& foo : obj_list.get_data()) {
        base::log_msg(std::to_string(ch.get(foo) * 4. / Context::get_worker_info()->get_num_workers()));
    }
}

void test_push() {
    base::log_msg("test: " + Context::get_param("log_dir"));
    ObjList<Foo> obj_list;
    auto& ch = ChannelFactory::create_push_channel<double>(obj_list, obj_list);
    double num_pts_inside = 0;
    int tot_pts = 10000;
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    for (int i = 0; i < tot_pts; i++) {
        double x = distribution(generator);
        double y = distribution(generator);
        if (x * x + y * y <= 1.)
            num_pts_inside += 1;
    }
    double pi = num_pts_inside / tot_pts;
    ch.push(pi, 0);
    ch.flush();
    ch.prepare_messages();
    for (Foo& foo : obj_list.get_data()) {
        double tot = 0;
        for (double pi : ch.get(foo))
            tot += pi;
        base::log_msg(std::to_string((tot / ch.get(foo).size()) * 4.));
    }
}

int main(int argc, char** argv) {
    init_with_args(argc, argv, {"log_dir"});
    run_job(test_push);
    return 0;
}
