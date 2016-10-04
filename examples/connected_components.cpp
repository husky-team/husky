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
#include "lib/aggregator_factory.hpp"

class Vertex {
   public:
    using KeyT = int;

    Vertex() = default;
    explicit Vertex(const KeyT& id) : vertexId(id), color(id) {}
    const KeyT& id() const { return vertexId; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& u) {
        stream << u.vertexId << u.adj << u.color;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& u) {
        stream >> u.vertexId >> u.adj >> u.color;
        return stream;
    }

    int vertexId;
    std::vector<int> adj;
    int color = -1;
};

void cc() {
    husky::io::HDFSLineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    // Create and globalize vertex objects
    auto& vertex_list = husky::ObjListFactory::create_objlist<Vertex>();
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
        husky::ChannelFactory::create_push_combined_channel<int, husky::MinCombiner<int>>(vertex_list, vertex_list);
    // Aggregator to check how many vertexes updating
    husky::lib::Aggregator<int> agg(0, [](int& a, const int& b) { a += b; });
    agg.to_reset_each_iter();
    auto& agg_ch = husky::lib::AggregatorFactory::get_channel();

    // Initialization
    husky::list_execute(vertex_list, {}, {&ch, &agg_ch}, [&ch, &agg](Vertex& u) {
        // Get the smallest color among neighbors
        for (auto nb : u.adj) {
            if (nb < u.color)
                u.color = nb;
        }
        if (u.color < u.id())
            agg.update(1);
        // Broadcast my color
        for (auto nb : u.adj) {
            if (nb > u.color)
                ch.push(u.color, nb);
        }
    });
    // Main Loop
    while (agg.get_value()) {
        if (husky::Context::get_global_tid() == 0)
            husky::base::log_msg("# updated in this round: "+std::to_string(agg.get_value()));
        husky::list_execute(vertex_list, {&ch}, {&ch, &agg_ch}, [&ch, &agg](Vertex& u) {
            if (ch.has_msgs(u)) {
                auto c = ch.get(u);
                if (c < u.color) {
                    u.color = c;
                    agg.update(1);
                    for (auto nb : u.adj)
                        ch.push(u.color, nb);
                }
            }
        });
    }
    constexpr bool small_graph = true;
    if (small_graph) {
        husky::list_execute(vertex_list, [](Vertex& u) {
            husky::base::log_msg("vertex: "+std::to_string(u.id()) + " color: "+std::to_string(u.color));
        });
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(cc);
        return 0;
    }
    return 1;
}
