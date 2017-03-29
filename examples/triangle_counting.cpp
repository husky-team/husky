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
#include <vector>

#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"

class Vertex {
   public:
    using KeyT = int;

    Vertex() = default;
    explicit Vertex(const KeyT& id) : vertexId(id) {}
    const KeyT& id() const { return vertexId; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& u) {
        stream << u.vertexId << u.adj;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& u) {
        stream >> u.vertexId >> u.adj;
        return stream;
    }

    int vertexId;
    std::vector<int> adj;
};

void triangle_counting() {
    auto& infmt = husky::io::InputFormatStore::create_line_inputformat();
    infmt.set_input(husky::Context::get_param("input"));

    // Create and globalize vertex objects
    auto& vertex_list = husky::ObjListStore::create_objlist<Vertex>();
    auto parse_vertex = [&vertex_list](boost::string_ref chunk) {
        if (chunk.size() == 0)
            return;

        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        int id = std::stoi(*it++);
        it++;
        Vertex v(id);
        while (it != tok.end()) {
            v.adj.push_back(stoi(*it++));
        }
        std::sort(v.adj.begin(), v.adj.end());
        vertex_list.add_object(std::move(v));
    };
    husky::load(infmt, parse_vertex);
    husky::globalize(vertex_list);

    // Iterative Triangle Counting computation
    auto& ch = husky::ChannelStore::create_push_channel<int>(vertex_list, vertex_list);
    husky::lib::Aggregator<int> count;
    auto& agg_ch = husky::lib::AggregatorFactory::get_channel();

    int numIters = stoi(husky::Context::get_param("tc_iters"));
    size_t batch_size = (size_t) std::ceil(((double) vertex_list.get_size()) / numIters);
    for (int iter = 0; iter < numIters; ++iter) {
        husky::list_execute(vertex_list, {}, {&ch}, [&ch, &vertex_list, iter, batch_size](Vertex& u) {
            size_t idx = vertex_list.index_of(&u);
            if (idx >= iter * batch_size && idx < (iter + 1) * batch_size) {
                int id = u.id();
                size_t u_adj_size = u.adj.size();
                size_t j = 0;
                if (u_adj_size > 0) {
                    while (u.adj[j] <= id) {
                        ++j;
                    }
                }
                for (j; j < u_adj_size; j++) {
                    for (size_t k = j + 1; k < u_adj_size; k++) {
                        ch.push(u.adj[j], u.adj[k]);
                    }
                }
            }
        });
        husky::list_execute(vertex_list, {&ch}, {&agg_ch}, [&ch, &count](Vertex& u) {
            auto& msgs = ch.get(u);
            if (!msgs.empty()) {
                for (auto& msg : msgs) {
                    if (std::binary_search(u.adj.begin(), u.adj.end(), msg)) {
                        count.update(1);
                    }
                }
            }
        });
        if (husky::Context::get_global_tid() == 0) husky::LOG_I << "iter " << iter;
    }
    if (husky::Context::get_global_tid() == 0) {
        husky::LOG_I << "Number of triangles = " << count.get_value();
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("tc_iters");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(triangle_counting);
        return 0;
    }
    return 1;
}
