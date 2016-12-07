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

#include "io/input/mongodb_inputformat.hpp"

#include <string>
#include <vector>

#include "mongo/bson/bson.h"
#include "mongo/client/dbclient.h"

#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/coordinator.hpp"

namespace husky {
namespace io {

enum MongoDBInputFormatSetUp {
    NotSetUp = 0,
    NSSetUp = 1 << 1,
    ServerSetUp = 1 << 2,
    // TODO(legend): how to check if needs auth.
    AuthSetUp = 1 << 2,
    QuerySetUp = 1 << 2,
    AllSetUp = NSSetUp | ServerSetUp | AuthSetUp | QuerySetUp,
};

MongoDBInputFormat::MongoDBInputFormat() {
    mongo::client::initialize();
    records_vector_.clear();
    is_setup_ = MongoDBInputFormatSetUp::NotSetUp;
}

MongoDBInputFormat::~MongoDBInputFormat() { records_vector_.clear(); }

void MongoDBInputFormat::set_ns(const std::string& database, const std::string& collection) {
    database_ = database;
    collection_ = collection;
    ns_ = database_ + "." + collection_;
    is_setup_ |= MongoDBInputFormatSetUp::NSSetUp;
}

void MongoDBInputFormat::set_server(const std::string& server) {
    server_ = server;
    is_setup_ |= MongoDBInputFormatSetUp::ServerSetUp;
}

void MongoDBInputFormat::set_auth(const std::string& username, const std::string& password) {
    username_ = username;
    password_ = password;
    need_auth_ = true;
    is_setup_ |= MongoDBInputFormatSetUp::AuthSetUp;
}

void MongoDBInputFormat::set_query(const mongo::Query& query) {
    query_ = query;
    is_setup_ |= MongoDBInputFormatSetUp::QuerySetUp;
}

bool MongoDBInputFormat::is_setup() const { return !(is_setup_ ^ MongoDBInputFormatSetUp::AllSetUp); }

void MongoDBInputFormat::ask_split() {
    BinStream question;
    question << need_auth_;
    if (need_auth_)
        question << server_ << ns_ << username_ << password_ << husky::Context::get_param("hostname");
    else
        question << server_ << ns_ << husky::Context::get_param("hostname");

    BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_MONGODB_REQ);
    answer >> split_;
    return;
}

void MongoDBInputFormat::read() {
    if (!records_vector_.empty())
        records_vector_.clear();

    mongo::DBClientConnection client;
    client.connect(split_.get_input_uri(), error_msg_);

    if (need_auth_)
        client.auth(database_, username_, password_, error_msg_);

    query_.minKey(mongo::fromjson(split_.get_min()));
    query_.maxKey(mongo::fromjson(split_.get_max()));

    auto cursor = client.query(split_.get_ns(), query_);
    while (cursor->more()) {
        mongo::BSONObj o = cursor->next();
        records_vector_.push_back(o.jsonString());
    }

    return;
}

void MongoDBInputFormat::send_end() {
    BinStream question;
    question << split_;
    husky::Context::get_coordinator()->ask_master(question, husky::TYPE_MONGODB_END_REQ);
    return;
}

bool MongoDBInputFormat::next(RecordT& ref) {
    while (records_vector_.empty()) {
        ask_split();
        if (!split_.is_valid())
            return false;
        read();
        if (records_vector_.empty())
            send_end();
    }

    ref = records_vector_.back();
    records_vector_.pop_back();
    if (records_vector_.empty())
        send_end();
    return true;
}

}  // namespace io
}  // namespace husky
