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
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"

class Vertex {
   public:
    using KeyT = int;

    Vertex() = default;
    explicit Vertex(const KeyT& id) : vertex_id(id), cid(id) {}
    const KeyT& id() const { return vertex_id; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& v) {
        stream << v.vertex_id << v.adj << v.cid;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& v) {
        stream >> v.vertex_id >> v.adj >> v.cid;
        return stream;
    }

    int vertex_id;
    std::vector<int> adj;
    int cid = -1;
};

void cc() {
    auto& infmt = husky::io::InputFormatStore::create_line_inputformat();
    infmt.set_input(husky::Context::get_param("input"));

    // Create and globalize vertex objects
    auto& vertex_list = husky::ObjListStore::create_objlist<Vertex>();
    auto parse_wc = [&vertex_list](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        int id = stoi(*it++);
        it++;
        Vertex v(id);
        while (it != tok.end()) {
            v.adj.push_back(stoi(*it++));
        }
        vertex_list.add_object(std::move(v));
    };
    husky::load(infmt, parse_wc);
    husky::globalize(vertex_list);

    auto& ch =
        husky::ChannelStore::create_push_combined_channel<int, husky::MinCombiner<int>>(vertex_list, vertex_list);
    // Aggregator to check how many vertexes updating
    husky::lib::Aggregator<int> not_finished(0, [](int& a, const int& b) { a += b; });
    not_finished.to_reset_each_iter();
    not_finished.update(1);

    auto& agg_ch = husky::lib::AggregatorFactory::get_channel();

    // Initialization
    husky::list_execute(vertex_list, {}, {&ch, &agg_ch}, [&ch, &not_finished](Vertex& v) {
        // Get the smallest component id among neighbors
        for (auto nb : v.adj) {
            if (nb < v.cid)
                v.cid = nb;
        }
        // Broadcast my component id
        for (auto nb : v.adj) {
            if (nb > v.cid)
                ch.push(v.cid, nb);
        }
    });
    // Main Loop
    while (not_finished.get_value()) {
        if (husky::Context::get_global_tid() == 0)
            husky::base::log_msg("# updated in this round: "+std::to_string(not_finished.get_value()));
        husky::list_execute(vertex_list, {&ch}, {&ch, &agg_ch}, [&ch, &not_finished](Vertex& v) {
            if (ch.has_msgs(v)) {
                auto msg = ch.get(v);
                if (msg < v.cid) {
                    v.cid = msg;
                    not_finished.update(1);
                    for (auto nb : v.adj)
                        ch.push(v.cid, nb);
                }
            }
        });
    }
    std::string small_graph = husky::Context::get_param("print");
    if (small_graph == "1") {
        husky::list_execute(vertex_list, [](Vertex& v) {
            husky::base::log_msg("vertex: "+std::to_string(v.id()) + " component id: "+std::to_string(v.cid));
        });
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("print");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(cc);
        return 0;
    }
    return 1;
}
