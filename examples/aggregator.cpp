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

#include <string>
#include <utility>
#include <vector>

#include "boost/tokenizer.hpp"

#include "base/log.hpp"
#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"

namespace husky {

using base::log_msg;
using lib::Aggregator;
using lib::AggregatorFactory;

class Word {
   public:
    using KeyT = std::string;

    Word() = default;
    explicit Word(const KeyT& w) : word(w) {}
    const KeyT& id() const { return word; }

    KeyT word;
    int count = 0;
};

void aggregator() {
    auto id = Context::get_global_tid();  // [0, number of threads globally)
    {  // 1. an easy example
        Aggregator<int> agg;              // creation
        agg.update(id);                   // aggregate
        AggregatorFactory::sync();        // synchronize manually
        if (id == 0) {
            log_msg("Sum Calculation: " + std::to_string(agg.get_value()));  // get the value
        }
    }
    {  // 2. count average occurence of word, and find the word with maximum occurence
        Aggregator<int> tot_word, tot_occurence;
        Aggregator<std::pair<std::string, int>> max_occurence(
            {"", 0},                                                                    // initial value
            [](std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {  // aggregation rule
                if (a.second < b.second) {
                    a = b;
                }
            });
        auto& ac = AggregatorFactory::get_channel();

        auto& infmt = husky::io::InputFormatStore::create_line_inputformat();
        infmt.set_input(husky::Context::get_param("input"));

        auto& word_list = ObjListStore::create_objlist<Word>();
        auto& ch = ChannelStore::create_push_combined_channel<int, SumCombiner<int>>(infmt, word_list);

        load(infmt, {&ch}, [&](boost::string_ref& chunk) {
            if (chunk.size() == 0)
                return;
            boost::char_separator<char> sep(" \t");
            boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
            for (auto& w : tok) {
                ch.push(1, w);
            }
        });

        // To synchronize using aggregator channel
        list_execute(word_list, {&ch}, {&ac}, [&](Word& word) {
            int o = ch.get(word);
            tot_word.update(1);
            tot_occurence.update(o);
            max_occurence.update({word.id(), o});
        });

        if (id == 0) {
            log_msg("Average occurence of words in " + Context::get_param("input") + ": " +
                    std::to_string((tot_occurence.get_value() + 0.) / tot_word.get_value()));
            log_msg("Word with maximum occurence: (\"" + max_occurence.get_value().first + "\", " +
                    std::to_string(max_occurence.get_value().second) + ")");
        }
    }
}

}  // namespace husky

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(husky::aggregator);
        return 0;
    }
    return 1;
}
