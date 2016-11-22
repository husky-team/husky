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

#include "base/log.hpp"
#include "core/engine.hpp"
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

void test_load() {
    std::string hostname = husky::Context::get_param("hostname");
    std::string port = husky::Context::get_param("port");
    auto& infmt = husky::io::InputFormatStore::create_flume_inputformat(hostname, std::stoi(port));

    infmt.start_listen();

    auto& word_list = husky::ObjListStore::create_objlist<Word>();
    auto& ch = husky::ChannelStore::create_push_combined_channel<int, husky::SumCombiner<int>>(infmt, word_list);

    auto parse_wc = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        for (auto& w : tok) {
            ch.push(1, w);
        }
    };

    for (int i = 0; i < 10000000; ++i) {
        husky::load(infmt, parse_wc);
        husky::list_execute(word_list, [&ch](Word& word) {
            int sum = ch.get(word);
            if (sum == 0)
                return;
            else
                husky::base::log_msg(word.word + ": " + std::to_string(sum));
        });
    }
}

int main(int argc, char ** argv) {
    std::vector<std::string> args;
    // if hostname = "h1", listen on machine h1
    // if hostname = "localhost", listen all machines
    args.push_back("hostname");
    args.push_back("port");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(test_load);
        return 0;
    }
    return 1;
}
