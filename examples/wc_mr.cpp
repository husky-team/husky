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
#include <vector>

#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "io/input/hdfs_line_inputformat.hpp"
#include "io/input/mongodb_inputformat.hpp"
#include "io/input/nfs_line_inputformat.hpp"

using namespace husky;

class Word {
   public:
    using KeyT = std::string;

    Word() = default;
    Word(const KeyT& w) : word(w) {}
    const KeyT& id() const { return word; }

    KeyT word;
    int count = 0;
};

void wc() {
    // choose an inputformat
    io::HDFSLineInputFormat infmt;
    infmt.set_input(Context::get_param("input"));

    auto& word_list = ObjListFactory::create_objlist<Word>();
    auto& ch = ChannelFactory::create_push_combined_channel<int, SumCombiner<int>>(infmt, word_list);

    auto parse_wc = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        for (auto& w : tok) {
            ch.push(1, w);
        }
    };

    // Just a showcase of two types of list_execute
    enum class ListExecuteStyle { simple, precise };
    ListExecuteStyle style = ListExecuteStyle::simple;
    if (style == ListExecuteStyle::simple) {
        // This_list execute style is simple and direct
        load(infmt, parse_wc);
        list_execute(word_list, [&ch](Word& word) { base::log_msg(word.word + ": " + std::to_string(ch.get(word))); });
    } else if (style == ListExecuteStyle::precise) {
        // This_list execute is precise. Need to decide which channels to be used as in/out channels
        load(infmt, {&ch}, parse_wc);
        list_execute(word_list, {&ch}, {},
                     [&ch](Word& word) { base::log_msg(word.word + ": " + std::to_string(ch.get(word))); });
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    if (init_with_args(argc, argv, args)) {
        run_job(wc);
        return 0;
    }
    return 1;
}
