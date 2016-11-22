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

#include <functional>
#include <iostream>
#include <string>

#include "boost/tokenizer.hpp"
#include "mongo/bson/bson.h"
#include "mongo/client/dbclient.h"

#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "io/output/mongodb_outputformat.hpp"

void test() {
    std::string server = husky::Context::get_param("mongo_server");
    std::string db = husky::Context::get_param("mongo_db");
    std::string collection = husky::Context::get_param("mongo_collection");
    // std::string user = husky::Context::get_param("mongo_user");
    // std::string pwd = husky::Context::get_param("mongo_pwd");

    auto& inputformat = husky::io::InputFormatStore::create_mongodb_inputformat();
    inputformat.set_server(server);
    inputformat.set_ns(db, collection);
    // inputformat.set_auth(user, pwd);

    husky::io::MongoDBOutputFormat outputformat;
    outputformat.set_server(server);
    outputformat.set_ns(db, collection + "_copy");
    // outputformat.set_auth(user, pwd);

    auto read_and_write = [&](std::string& chunk) {
        mongo::BSONObj o = mongo::fromjson(chunk);
        outputformat.commit(o.removeField("_id").jsonString());
    };

    husky::load(inputformat, read_and_write);
    outputformat.flush_all();

    husky::base::log_msg("Done");
}

int main(int argc, char** argv) {
    ASSERT_MSG(husky::init_with_args(argc, argv, {"mongo_server", "mongo_db", "mongo_collection"}), "Wrong arguments!");
    husky::run_job(test);
    return 0;
}
