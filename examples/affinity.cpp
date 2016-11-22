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

#include "lib/vector.hpp"

typedef husky::lib::DenseVector<float> ndvec;

class NDpoint {
    // N-dimension point modelling the input data
   public:
    using KeyT = int;

    NDpoint() {}
    explicit NDpoint(const KeyT& w) : pointId(w) {}
    NDpoint(const KeyT& pId, ndvec coords) {
        this->pointId = pId;
        this->coords = std::move(coords);
    }
    virtual const KeyT& id() const { return pointId; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, NDpoint u) {
        stream << u.pointId << u.coords;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, NDpoint& u) {
        stream >> u.pointId >> u.coords;
        return stream;
    }

    int pointId;
    ndvec coords;
};

void affinity() {
    husky::io::LineInputFormat infmt;
    infmt.set_input(husky::Context::get_param("input"));

    int dimension = std::stoi(husky::Context::get_param("dimension"));

    // for calculating mean(x_i  ) for each dimension i in input data
    husky::lib::Aggregator<ndvec> dataxAgg(ndvec(dimension, 0.), [](ndvec& a, const ndvec& b) { a += b; },
                                           [&](ndvec& v) { v = std::move(ndvec(dimension, 0.)); });

    // for calculating mean(x_i^2) for each dimension i in input data
    husky::lib::Aggregator<ndvec> dataxxAgg(ndvec(dimension, 0.), [](ndvec& a, const ndvec& b) { a += b; },
                                            [&](ndvec& v) { v = std::move(ndvec(dimension, 0.)); });

    husky::lib::Aggregator<int> dataCountAgg;  // total no. of rows in input
    auto& ac = husky::lib::AggregatorFactory::get_channel();

    // 1. Create and globalize ndpoint objects
    auto& ndpoint_list = husky::ObjListStore::create_objlist<NDpoint>();
    ndvec coordsSq(dimension);
    auto parse_ndpoint = [&](boost::string_ref& chunk) {
        if (chunk.size() == 0)
            return;
        boost::char_separator<char> sep(" \t");
        boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
        boost::tokenizer<boost::char_separator<char>>::iterator it = tok.begin();
        int id = stoi(*it++);
        ndvec coords(dimension);
        for (int i = 0; i < dimension; i++) {
            coords[i] = stod(*it++);
            coordsSq[i] = coords[i] * coords[i];
        }
        dataCountAgg.update(1);
        dataxAgg.update(coords);
        dataxxAgg.update(coordsSq);
        ndpoint_list.add_object(NDpoint(id, coords));
    };
    load(infmt, {&ac}, parse_ndpoint);
    globalize(ndpoint_list);
    int totPts = dataCountAgg.get_value();
    husky::base::log_msg("No. of rows in input " + std::to_string(totPts));
    husky::base::log_msg("No. of rows in local " + std::to_string(ndpoint_list.get_size()));

    auto& allPtsCh = husky::ChannelStore::create_broadcast_channel<int, NDpoint>(ndpoint_list);
    list_execute(ndpoint_list, [&](NDpoint& p) { allPtsCh.broadcast(p.id(), p); });

    // 2. calculate the variance used in similarity function
    size_t totDataPoints = dataCountAgg.get_value();
    ndvec dataMean = dataxAgg.get_value() / totDataPoints;
    ndvec dataVar = dataxxAgg.get_value() / totDataPoints;
    // Var(x) = <x^2> - <x><x>
    for (int i = 0; i < dimension; ++i) {
        dataVar[i] -= dataMean[i] * dataMean[i];
    }

    // 3. calculate the affinity matrix
    // Since affinity matrix is symmetric, we have A(i,j) = A(j,i)
    // However we duplicate the calculation here
    // Calculating the same value twice is still faster then cal once then communicate to other node (for 10D data)
    list_execute(ndpoint_list, [&](NDpoint& p) {
        ndvec elems(totPts, 0.);
        for (uint i = 0; i < totPts; i++) {
            if (i == p.id())
                continue;
            auto& p2 = allPtsCh.get(i);  // we assume the datapoints ID start from 0 to N with no gaps
            float nSigmaSq = 0;
            for (uint j = 0; j < dimension; ++j) {
                nSigmaSq += pow(p.coords[j] - p2.coords[j], 2) / (2. * dataVar[j]);
            }
            elems[i] = exp(-nSigmaSq);
        }

        // output to file
        std::string log;
        log = std::to_string(p.id());
        for (int i = 0; i < totPts; ++i) {
            log += "  " + std::to_string(elems[i]);
        }
        log += "\n";
        husky::io::HDFS::Write("master", "9000", log, husky::Context::get_param("outDir"),
                               husky::Context::get_global_tid());
    });
}

int main(int argc, char** argv) {
    // Input:
    // A txt with the each line as
    // pointID dim1Value dim2Value dim3Value ...
    // eg.
    // 0 -2.4903068672 3.13023508816
    // 1 -0.788251940489 6.95547689798
    // 2 -1.11412555846 6.91076871556
    // ...
    //
    // Output:
    // A txt with each line containing 1 row of the affinity matrix as
    // rowID rowElem1 rowElem2 rowElem3 ...rowElemN
    std::vector<std::string> args;
    args.push_back("hdfs_namenode");
    args.push_back("hdfs_namenode_port");
    args.push_back("input");      // path to input file eg. hdfs:///user/ylchan/testPICdata.txt
    args.push_back("outDir");     // output dir in hdfs eg. /user/ylchan/AffMat_T2/
    args.push_back("dimension");  // number of dimensions for the input data
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(affinity);
        return 0;
    }
    return 1;
}
