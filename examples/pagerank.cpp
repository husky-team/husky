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

class Vertex {
   public:
    using KeyT = int;

    Vertex() : pr_(0.15) {}
    explicit Vertex(const KeyT& w) : vertexId_(w) { pr_ = 0.15; }
    Vertex(const KeyT& vId, std::vector<int> adj, float pr = 0.15) {
        vertexId_ = vId;
        adj_ = std::move(adj);
        pr_ = pr;
    }
    virtual const KeyT& id() const { return vertexId_; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, Vertex u) {
        stream << u.vertexId_ << u.adj_ << u.pr_;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& u) {
        stream >> u.vertexId_ >> u.adj_ >> u.pr_;
        return stream;
    }

    int vertexId_;
    std::vector<int> adj_;
    float pr_;
};

void pagerank() {
    io::HDFSLineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    // Create and globalize vertex objects
    husky::ObjList<Vertex> vertex_list;
    auto parse_wc = [&vertex_list](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        int id = stoi(*it++);
        it++;
        std::vector<int> adj;
        while (it != tok.end()) {
            adj.push_back(stoi(*it++));
        }
        vertex_list.add_object(Vertex(id, adj, 0.15));
    };
    load(infmt, parse_wc);
    globalize(vertex_list);

    // Iterative PageRank computation
    auto& prch =
        husky::ChannelFactory::create_push_combined_channel<float, husky::SumCombiner<float>>(vertex_list, vertex_list);
    int numIters = stoi(husky::Context::get_param("iters"));
    for (int iter = 0; iter < numIters; ++iter) {
        list_execute(vertex_list, [&prch, &it](Vertex& u) {
            if (it > 0)
                u.pr_ = 0.85 * prch.get(u) + 0.15;
            if (u.adj_.size() == 0)
                return;

            float sendPR = u.pr_ / u.adj_.size();
            for (auto& nb : u.adj_) {
                prch.push(sendPR, nb);
            }
        });
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("iters");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(pagerank);
        return 0;
    }
    return 1;
}
