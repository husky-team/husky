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

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "boost/tokenizer.hpp"

#include "base/serialization.hpp"
#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"

class Word {
   public:
    using KeyT = std::string;

    Word() = default;
    explicit Word(const KeyT& w) : word(w) {}
    const KeyT& id() const { return word; }

    KeyT word;
    int count = 0;
};

bool operator<(const std::pair<int, std::string>& a, const std::pair<int, std::string>& b) {
    return a.first == b.first ? a.second < b.second : a.first < b.first;
}

void wc() {
    std::string server(husky::Context::get_param("elasticsearch_server"));
    std::string index(husky::Context::get_param("elasticsearch_index"));
    std::string type(husky::Context::get_param("elaticsearch_type"));
    auto& infmt = husky::io::InputFormatStore::create_elasticsearch_inputformat();
    infmt.set_server(server);
    std::string query(" { \"query\": { \"match_all\":{}}}");
    infmt.scan_fully(index, type, query, 1000);
    auto& word_list = husky::ObjListStore::create_objlist<Word>();
    auto& ch = husky::ChannelStore::create_push_combined_channel<int, husky::SumCombiner<int>>(infmt, word_list);

    auto parse_wc = [&](std::string& chunk) {
        if (chunk.size() == 0)
            return;
        boost::property_tree::ptree pt;
        std::stringstream ss(chunk);
        read_json(ss, pt);
        std::string content = pt.get_child("_source").get<std::string>("content");
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(content, sep);
        for (auto& w : tok) {
            ch.push(1, w);
        }
    };
    husky::load(infmt, parse_wc);

    // Show topk words.
    const int kMaxNum = 100;
    typedef std::set<std::pair<int, std::string>> TopKPairs;
    auto add_to_topk = [](TopKPairs& pairs, const std::pair<int, std::string>& p) {
        if (pairs.size() == kMaxNum && *pairs.begin() < p)
            pairs.erase(pairs.begin());
        if (pairs.size() < kMaxNum)
            pairs.insert(p);
    };
    husky::lib::Aggregator<TopKPairs> unique_topk(
        TopKPairs(),
        [add_to_topk](TopKPairs& a, const TopKPairs& b) {
            for (auto& i : b) {
                add_to_topk(a, i);
            }
        },
        [](TopKPairs& a) { a.clear(); },
        [add_to_topk](husky::base::BinStream& in, TopKPairs& pairs) {
            pairs.clear();
            for (size_t n = husky::base::deser<size_t>(in); n--;)
                add_to_topk(pairs, husky::base::deser<std::pair<int, std::string>>(in));
        },
        [](husky::base::BinStream& out, const TopKPairs& pairs) {
            out << pairs.size();
            for (auto& p : pairs)
                out << p;
        });

    husky::list_execute(word_list, [&ch, &unique_topk, add_to_topk](Word& word) {
        unique_topk.update(add_to_topk, std::make_pair(ch.get(word), word.id()));
    });

    husky::lib::AggregatorFactory::sync();

    if (husky::Context::get_global_tid() == 0) {
        for (auto& i : unique_topk.get_value())
            husky::LOG_I << i.second << " " << i.first;
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("elasticsearch_server");
    args.push_back("elasticsearch_index");
    args.push_back("elasticsearch_type");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(wc);
        return 0;
    }
    return 1;
}
