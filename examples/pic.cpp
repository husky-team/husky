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

#include <random>
#include <string>
#include <vector>

#include "boost/tokenizer.hpp"

#include "core/engine.hpp"
#include "io/hdfs_manager.hpp"
#include "io/input/line_inputformat.hpp"
#include "lib/aggregator_factory.hpp"

typedef std::vector<float> edgeWvec;

class Node {
    // A node storing node value and connected edges (node value forms the classification vector)
   public:
    using KeyT = int;

    Node() {}
    explicit Node(const KeyT& p) : pos(p), edgeW(), val(0.), valp(0.), valpp(0.) {}
    Node(const KeyT& p, edgeWvec edgeW, float edgeWsum) {
        this->pos = p;
        this->edgeWsum = edgeWsum;
        this->edgeW = std::move(edgeW);
        this->val = 0.;
        this->valp = 0.;
        this->valpp = 0.;
    }
    virtual const KeyT& id() const { return pos; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, Node u) {
        stream << u.pos << u.edgeW << u.edgeWsum << u.val << u.valp << u.valpp << u.valtmp;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Node& u) {
        stream >> u.pos >> u.edgeW >> u.edgeWsum >> u.val >> u.valp >> u.valpp >> u.valtmp;
        return stream;
    }

    KeyT pos;

    edgeWvec edgeW;
    float edgeWsum;

    float val;
    float valp;    // val at previous iteration
    float valpp;   // val at previousX2 iteration
    float valtmp;  // val before normalize

    void setVal(float newval) {
        valpp = valp;
        valp = val;
        val = newval;
    }

    // Accelation of node value change, for deciding when to stop iter
    float getAcc() { return fabs(fabs(val - valp) - fabs(valp - valpp)); }
};

void pic() {
    husky::io::LineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    husky::lib::Aggregator<int> dataCountAgg;    // total no. of rows in input affinity matrix
    husky::lib::Aggregator<float> affMatVolAgg;  // sum of elements' val in affinity matrix
    auto& ac = husky::lib::AggregatorFactory::get_channel();

    // 1. Create and globalize node objects
    auto& node_list = husky::ObjListStore::create_objlist<Node>();
    auto parse_affMatRow = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        int id = stoi(*it++);
        float edgeWsum = 0.;
        edgeWvec edgeW;
        while (it != tok.end()) {
            float tmp = stod(*it++);
            edgeW.push_back(tmp);
            edgeWsum += tmp;
        }
        edgeW[id] = 0.0;
        edgeW.shrink_to_fit();

        dataCountAgg.update(1);
        affMatVolAgg.update(edgeWsum);
        node_list.add_object(Node(id, edgeW, edgeWsum));
    };
    load(infmt, {&ac}, parse_affMatRow);
    husky::globalize(node_list);

    int totDataPoints = dataCountAgg.get_value();
    husky::base::log_msg("node_list size " + std::to_string(totDataPoints));

    // 2. Init guess as suggested by paper
    list_execute(node_list, [&](Node& n) {
        assert(n.edgeW.size() == totDataPoints);
        n.setVal(n.edgeWsum / affMatVolAgg.get_value());
    });

    // 3. repeat multiply the affinity matrix to the vector till convergence
    // The Mat multiplication is same as each node doing a weighted average of its connected neighbors' value

    husky::lib::Aggregator<float> maxFloatAgg(0., [](float& a, const float& b) {
        if (b > a)
            a = b;
    });
    husky::lib::Aggregator<float> normFacAgg(0.);
    auto& nodeTonodeCh =
        husky::ChannelStore::create_push_combined_channel<float, husky::SumCombiner<float>>(node_list, node_list);

    int maxIter = std::stoi(husky::Context::get_param("maxIter"));
    float epsilon = std::stod(husky::Context::get_param("stopThres"));

    int outPerNIter = std::stoi(husky::Context::get_param("outPerNIter"));

    if (maxIter < 0)
        maxIter = 10;
    if (epsilon < 0)
        epsilon = 1e-5 / totDataPoints;

    for (uint iter = 0; iter < maxIter; ++iter) {
        list_execute(node_list, [&](Node& n) {
            // here we assuemed the edge is bidirectional with same weight (a.k.a Affinity mat is symmetric)
            for (uint i = 0; i < totDataPoints; ++i) {
                nodeTonodeCh.push(n.edgeW[i] * n.val, i);
            }
        });

        list_execute(node_list, {&nodeTonodeCh}, {&ac}, [&](Node& n) {
            n.valtmp = n.edgeWsum > 0 ? nodeTonodeCh.get(n) / n.edgeWsum : 0.0;
            normFacAgg.update(n.valtmp);
        });

        list_execute(node_list, {}, {&ac}, [&](Node& n) {
            n.setVal(n.valtmp / normFacAgg.get_value());
            maxFloatAgg.update(n.getAcc());
            if (outPerNIter > 0 && iter % outPerNIter == 0) {
                std::ostringstream oss;
                oss.precision(17);
                oss << n.id() << " " << n.val << "\n";
                husky::io::HDFS::Write("master", "9000", oss.str(),
                                       husky::Context::get_param("outDir") + "/iter" + std::to_string(iter),
                                       husky::Context::get_global_tid());
            }
        });

        if (husky::Context::get_global_tid() == 0) {
            husky::base::log_msg("Iter " + std::to_string(iter) + " " + "MaxAcc " +
                                 std::to_string(maxFloatAgg.get_value()) + " " + "log10(MaxAcc) " +
                                 std::to_string(log10(maxFloatAgg.get_value())));
        }

        if (maxFloatAgg.get_value() < epsilon)
            break;
        maxFloatAgg.to_reset_each_iter();
        normFacAgg.to_reset_each_iter();
    }  // end iter loop

    // 4. output
    list_execute(node_list, [&](Node& n) {
        std::ostringstream oss;
        oss.precision(17);
        oss << n.id() << " " << n.val << "\n";
        husky::io::HDFS::Write("master", "9000", oss.str(), husky::Context::get_param("outDir") + "/fin",
                               husky::Context::get_global_tid());
    });
}

int main(int argc, char** argv) {
    // Input:
    // The affinity matrix output by affinity.cpp i.e.
    // A txt with each line containing 1 row of the affinity matrix as
    // rowID rowElem1 rowElem2 rowElem3 ...rowElemN
    //
    // Output:
    // A txt with the each line as
    // pointID classificationVal
    // eg.
    // 0 0.001
    // 1 0.0009
    // 2 0.0009
    // ...
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");        // path to input file eg. hdfs:///user/ylchan/AffMat_T2/merge
    args.push_back("outDir");       // output dir in hdfs eg. /user/ylchan/testPIC/
    args.push_back("maxIter");      // max no. of power iteration to do (setting <0 implies 10)
    args.push_back("stopThres");    // stopping threshold (setting <0 implies auto)
    args.push_back("outPerNIter");  // saving intermediate result every N iteration (<=0 implies turn off)
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(pic);
        return 0;
    }
    return 1;
}
