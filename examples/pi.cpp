// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <random>
#include <string>
#include <vector>

#include "core/engine.hpp"

class PIObject {
   public:
    typedef int KeyT;
    int key;

    explicit PIObject(KeyT key) { this->key = key; }

    const int& id() const { return key; }
};

void pi() {
    // Each thread generates 1000 points
    int num_pts_per_thread = 1000;
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_real_distribution<double> distribution(-1.0, 1.0);
    int cnt = 0;
    for (int i = 0; i < num_pts_per_thread; i++) {
        double x = distribution(generator);
        double y = distribution(generator);
        if (x * x + y * y <= 1) {
            cnt += 1;
        }
    }

    // Aggregate statistics to object 0
    auto& pi_list = husky::ObjListStore::create_objlist<PIObject>();
    auto& ch =
        husky::ChannelStore::create_push_combined_channel<int, husky::SumCombiner<int>>(pi_list, pi_list);
    ch.push(cnt, 0);
    ch.flush();
    list_execute(pi_list, [&](PIObject& obj) {
        int sum = ch.get(obj);
        int total_pts = num_pts_per_thread * husky::Context::get_worker_info()->get_num_workers();
        husky::base::log_msg(std::to_string(4.0 * sum / total_pts));
    });
}

int main(int argc, char** argv) {
    if (husky::init_with_args(argc, argv)) {
        husky::run_job(pi);
        return 0;
    }
    return 1;
}
