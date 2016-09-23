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
    husky::ObjList<PIObject> pi_list;
    auto& ch =
        husky::ChannelFactory::create_push_combined_channel<double, husky::SumCombiner<double>>(pi_list, pi_list);
    int total_pts = 1000;
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    int cnt = 0;
    for (int i = 0; i < total_pts; i++) {
        double x = distribution(generator);
        double y = distribution(generator);
        if (x * x + y * y <= 1) {
            cnt += 1;
        }
    }
    ch.push(cnt * 4.0 / total_pts, 0);
    ch.flush();
    list_execute(pi_list, [&](PIObject& pi) {
        float sum = ch.get(pi);
        base::log_msg(std::to_string(ch.get(pi) / husky::Context::get_worker_info()->get_num_workers()));
    });
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(pi);
        return 0;
    }
    return 1;
}
