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
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "io/output/elasticsearch_outputformat.hpp"
#include "lib/aggregator_factory.hpp"

void mongo_to_es() {
    auto& infmt = husky::io::InputFormatStore::create_mongodb_inputformat();
    infmt.set_server(husky::Context::get_param("mongo_server"));
    infmt.set_ns(husky::Context::get_param("mongo_db"), husky::Context::get_param("mongo_collection"));
    infmt.set_query("");
    std::string server = husky::Context::get_param("elasticsearch_server");
    std::string index = husky::Context::get_param("elasticsearch_index");
    std::string type = husky::Context::get_param("elasticsearch_type");

    husky::io::ElasticsearchOutputFormat outfmt;
    outfmt.set_server(server, true);
    outfmt.bulk_setbound(1000);
    auto work = [&](std::string& chunk) {
        mongo::BSONObj o = mongo::fromjson(chunk);
        o = o.removeField("_id");
        std::string id = o.getStringField("id");
        if (chunk.size() == 0)
            return;
        outfmt.bulk_add("index", index, type, id, o.jsonString());
    };

    husky::load(infmt, work);
    outfmt.bulk_flush();
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("mongo_server");
    args.push_back("mongo_db");
    args.push_back("mongo_collection");
    args.push_back("elasticsearch_server");
    args.push_back("elasticsearch_index");
    args.push_back("elasticsearch_type");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(mongo_to_es);
        return 0;
    }
    return 1;
}
