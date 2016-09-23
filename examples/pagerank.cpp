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

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "io/input/hdfs_line_inputformat.hpp"

class PRVertex {
public:
    float pr;
    typedef int KeyT;
    int key;

    PRVertex() {}
    explicit PRVertex(KeyT key) {
        this->key = key;
    }

    inline virtual KeyT const & id() const {
        return key;
    }

    virtual void key_init(KeyT key) {
        this->key = key;
    }

    virtual void add_neighbors(KeyT nbKey) {
        this->nbs.push_back(nbKey);
    }

    virtual std::vector<KeyT> & get_neighbors() {
        return this->nbs;
    }

    static uint64_t partition(const KeyT & key) {
        return key;
    }

    friend husky::BinStream & operator >> (husky::BinStream & stream, PRVertex & v) {
        stream >> v.pr >> v.nbs >> v.key;
        return stream;
    }

    friend husky::BinStream & operator << (husky::BinStream & stream, PRVertex v) {
        stream << v.pr << v.nbs << v.key;
        return stream;
    }

protected:
    std::vector<KeyT> nbs;
};

std::function<void(PRVertex &)> exec_pagerank(husky::ObjList<PRVertex> & obj_list, husky::PushCombinedChannel<float, PRVertex, husky::SumCombiner<float>> & ch) {
    std::function<void(PRVertex &)> exec_lambda = [&](PRVertex & v) {
        float sum = 0;
        sum += ch.get(v);
        v.pr = 0.15 + 0.85 * sum;

        if (v.get_neighbors().size() == 0) return;

        float pr_contrib = v.pr / v.get_neighbors().size();
        for (auto & nb : v.get_neighbors()) {
            ch.push(pr_contrib, nb);
        }
    };
    return exec_lambda;
}

class Foo {
public:
    typedef int KeyT;
    int key;

    explicit Foo(KeyT & key) {
        this->key = key;
    }

    const KeyT & id() const {
        return key;
    }
};

void pagerank() {
    io::HDFSLineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));
    husky::ObjList<PRVertex> pr_list;
    auto & ch = husky::ChannelFactory::create_push_combined_channel<float, husky::SumCombiner<float>>(pr_list, pr_list);
    auto parse_pr = [&](boost::string_ref & chunk) {
        if (chunk.size() == 0) return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        PRVertex obj;
        int i = 0;
        for (auto & v : tok) {
            i++;
            if (i == 1)
                obj.key_init(std::stoi(v));
            else if (i == 2);
            else
                obj.add_neighbors(std::stoi(v));
        }
        pr_list.add_object(obj);
    };

    husky::load(infmt, parse_pr);
    husky::globalize(pr_list);
    husky::list_execute(pr_list, [&](PRVertex & v) {
        // send dummy page rank values
        for (auto & nb : v.get_neighbors()) {
            ch.push(0, nb);
        }
    });

    husky::list_execute(pr_list, exec_pagerank(pr_list, ch), std::stoi(husky::Context::get_param("num_iters")));

    // The following part collects pagerank values and output the top k id-value pairs to the console
    husky::ObjList<Foo> foo_list;
    auto & foo_channel = husky::ChannelFactory::create_push_combined_channel<std::string, husky::SumCombiner<std::string>>(foo_list, foo_list);
    husky::list_execute(pr_list, [&](PRVertex & v) {
        std::string msg = std::to_string(v.id()) + "," + std::to_string(v.pr) + "\t";
        foo_channel.push(msg, 0);
    });
    foo_channel.flush();

    std::string msg = "";
    husky::list_execute(foo_list, [&](Foo & foo) {
        msg = foo_channel.get(foo);
    });
    if (msg != "") {
        std::vector<std::pair<int, double> > id_pr_list;
        boost::char_separator<char> sep(",\t");
        boost::tokenizer<boost::char_separator<char>> tok(msg, sep);
        int i = 0;
        int id_value;
        for (auto & value : tok) {
            if (i % 2 == 0)
                id_value = std::stoi(value);
            else
                id_pr_list.push_back(std::make_pair(id_value, std::stod(value)));
            ++i;
        }
        std::sort(id_pr_list.begin(), id_pr_list.end(), [](auto & left, auto & right) {
            return left.second > right.second;
        });
        base::log_msg("The top " + husky::Context::get_param("topk") + " pagerank id value pair are");
        for (int j = 0; j < std::stoi(husky::Context::get_param("topk")); j++) {
            base::log_msg(std::to_string(id_pr_list[j].first) + " : " + std::to_string(id_pr_list[j].second));
        }
    }
}

int main(int argc, char **argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("num_iters");
    args.push_back("topk");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(pagerank);
        return 0;
    }
    return 1;
}
