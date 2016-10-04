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

#include "boost/tokenizer.hpp"
#include "mongo/bson/bson.h"
#include "mongo/client/dbclient.h"

#include "base/serialization.hpp"
#include "core/engine.hpp"
#include "io/input/mongodb_inputformat.hpp"
#include "lib/aggregator_factory.hpp"

namespace husky {

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

class TopKPairs {
   public:
    TopKPairs() {}

    friend bool operator<(const std::pair<int, std::string>& a, const std::pair<int, std::string>& b) {
        return a.first == b.first ? a.second < b.second : a.first < b.first;
    }

    void push(const std::pair<int, std::string>& p) {
        if (tops.size() == kMaxNum && *tops.begin() < p) tops.erase(tops.begin());
        if (tops.size() < kMaxNum) tops.insert(p);
    }

    friend base::BinStream& operator>>(base::BinStream& in, TopKPairs& b) {
        size_t n;
        in >> n;
        b.tops.clear();
        for (std::pair<int, std::string> p; n--;) {
            in >> p;
            b.push(p);
        }
        return in;
    }

    friend base::BinStream& operator<<(base::BinStream& out, const TopKPairs& b) {
        out << b.tops.size();
        for (auto& i : b.tops) {
           out << i;
        }
        return out;
    }

    std::set<std::pair<int, std::string>> tops;
    static const int kMaxNum;
};

const int TopKPairs::kMaxNum = 100;

void wc() {
    io::MongoDBInputFormat infmt;
    infmt.set_server(Context::get_param("mongo_server"));
    infmt.set_ns(Context::get_param("mongo_db"), Context::get_param("mongo_collection"));
    infmt.set_query("");

    auto& word_list = ObjListFactory::create_objlist<Word>();
    auto& ch = ChannelFactory::create_push_combined_channel<int, SumCombiner<int>>(infmt, word_list);

    auto parse_wc = [&](std::string& chunk) {
        mongo::BSONObj o = mongo::fromjson(chunk);
        std::string content = o.getStringField("content");
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(content, sep);
        for (auto& w : tok) {
            ch.push(1, w);
        }
    };

    load(infmt, parse_wc);

    // Show topk words.
    Aggregator<TopKPairs> unique_topk(TopKPairs(),
        [](TopKPairs& a, const TopKPairs& b) {
            for (auto& i : b.tops)
                a.push(i);
        }, [](TopKPairs& a) { a.tops.clear(); });
    list_execute(word_list, [&ch, &unique_topk](Word& word) {
        unique_topk.update_any([&ch, &word](TopKPairs& p) {
            p.push({ch.get(word), word.id()});
        });
    });
    AggregatorFactory::sync();
    if (Context::get_global_tid() == 0)
      for (auto& i : unique_topk.get_value().tops)
          base::log_msg(i.second + " " + std::to_string(i.first));
}

}  // namespace husky

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("mongo_server");
    args.push_back("mongo_db");
    args.push_back("mongo_collection");
    husky::lib::AggregatorFactory::register_factory_constructor([]() {
        return new husky::lib::AggregatorFactory();
    });
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(husky::wc);
        return 0;
    }
    return 1;
}
