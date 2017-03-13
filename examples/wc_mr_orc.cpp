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

#include <sstream>
#include <string>
#include <vector>

#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "lib/aggregator_factory.hpp"
#include "io/input/inputformat_store.hpp"

class Word {
   public:
    using KeyT = std::string;

    Word() = default;
    explicit Word(const KeyT& w) : word(w) {}
    const KeyT& id() const { return word; }

    KeyT word;
    int count = 0;
};

void wc() {
    auto& infmt = husky::io::InputFormatStore::create_orc_inputformat();
    infmt.set_input(husky::Context::get_param("input"));
    husky::lib::Aggregator<int> num_tuple(0, [](int& a, const int& b){ a += b; });

    auto parse_wc = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        num_tuple.update(1);
    };
    husky::load(infmt, parse_wc);
    husky::lib::AggregatorFactory::sync();
    if (husky::Context::get_global_tid() == 0) {
        husky::LOG_I << "Total number of tuples: " << num_tuple.get_value();
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(wc);
        return 0;
    }
    return 1;
}
