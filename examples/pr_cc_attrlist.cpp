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
    explicit Vertex(const KeyT vid) : vertex_id(vid) {}
    const KeyT id() const { return vertex_id; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& v) {
        stream << v.vertex_id << v.neighbor;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& v) {
        stream >> v.vertex_id >> v.neighbor;
        return stream;
    }

    KeyT vertex_id;
    std::vector<KeyT> neighbor;
};

void pr_cc() {
    auto& infmt = husky::io::InputFormatStore::create_line_inputformat();
    infmt.set_input(husky::Context::get_param("input"));
    // Create vertex object list
    auto& vertex_list = husky::ObjListStore::create_objlist<Vertex>();
    // Create attribute lists for PageRank and ConnectedComponent
    auto& pr_list = vertex_list.create_attrlist<float>("pr");
    auto& cid_list = vertex_list.create_attrlist<int>("cid");

    // Load input
    auto parser = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        int id = stoi(*it++);
        it++;
        Vertex v(id);
        while (it != tok.end()) {
            v.neighbor.push_back(stoi(*it++));
        }
        auto idx = vertex_list.add_object(std::move(v));
        pr_list[idx] = 0.15;
        cid_list[idx] = id;
    };
    husky::load(infmt, parser);
    husky::globalize(vertex_list);
    if (husky::Context::get_global_tid() == 0)
        husky::base::log_msg("Loading input is done.");

    // Iterative PageRank computation
    auto& prch =
        husky::ChannelStore::create_push_combined_channel<float, husky::SumCombiner<float>>(vertex_list, vertex_list);
    int numIters = stoi(husky::Context::get_param("iters"));
    for (int iter = 0; iter < numIters; ++iter) {
        husky::list_execute(vertex_list, [&prch, &pr_list, iter](Vertex& v) {
            if (v.neighbor.size() == 0)
                return;
            if (iter > 0)
                pr_list[v] = 0.85 * prch.get(v) + 0.15;

            float send_pr = pr_list[v] / v.neighbor.size();
            for (auto& nb : v.neighbor) {
                prch.push(send_pr, nb);
            }
        });
    }
    if (husky::Context::get_global_tid() == 0)
        husky::base::log_msg("PageRank is done.");

    // Connected component computation
    // Computation is finished if there is no message
    husky::lib::Aggregator<int> not_finished;
    not_finished.update(1);
    not_finished.to_reset_each_iter();

    auto& agg_ch = husky::lib::AggregatorFactory::get_channel();
    auto& cidch =
        husky::ChannelStore::create_push_combined_channel<int, husky::MinCombiner<int>>(vertex_list, vertex_list);
    // Initilization
    husky::list_execute(vertex_list, {}, {&cidch, &agg_ch}, [&cidch, &cid_list, &not_finished](Vertex& v) {
        int& cid = cid_list[v];
        // Get the smallest component id among neighbors
        for (auto nb : v.neighbor) {
            if (nb < cid) {
                cid = nb;
            }
        }
        // Broadcast my component id
        for (auto nb : v.neighbor) {
            cidch.push(cid, nb);
        }
    });
    // Main loop
    while (not_finished.get_value()) {
        husky::list_execute(vertex_list, {&cidch}, {&cidch, &agg_ch}, [&cidch, &cid_list, &not_finished](Vertex& v) {
            if (cidch.has_msgs(v)) {
                int msg = cidch.get(v);
                // Broadcast again if the received component id is s
                if (msg < cid_list[v]) {
                    cid_list[v] = msg;
                    not_finished.update(1);
                    for (auto nb : v.neighbor) {
                        cidch.push(msg, nb);
                    }
                }
            }
        });
    }
    if (husky::Context::get_global_tid() == 0)
        husky::base::log_msg("ConnectComponent is done.");
    std::string small_graph = husky::Context::get_param("print");
    if (small_graph == "1") {
        husky::list_execute(vertex_list, [&cid_list](Vertex& v) {
            husky::base::log_msg("vertex: " + std::to_string(v.id()) + " component id: " + std::to_string(cid_list[v]));
        });
    }
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");
    args.push_back("iters");
    args.push_back("print");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(pr_cc);
        return 0;
    }
}
