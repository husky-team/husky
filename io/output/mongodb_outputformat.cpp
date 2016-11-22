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

#include "io/output/mongodb_outputformat.hpp"

#include <string>

#include "mongo/bson/bson.h"
#include "mongo/client/dbclient.h"

#include "base/exception.hpp"
#include "base/log.hpp"

namespace husky {
namespace io {

using base::log_msg;
using base::LOG_TYPE;

const int kMaxNumberOfRecord = 1024;

enum MongoDBOutputFormatSetUp {
    NotSetUp = 0,
    NSSetUp = 1 << 1,
    ServerSetUp = 1 << 2,
    AuthSetUp = 1 << 2,
    AllSetUp = NSSetUp | ServerSetUp | AuthSetUp,
};

MongoDBOutputFormat::MongoDBOutputFormat() {
    mongo::client::initialize();
    records_vector_.clear();
}

MongoDBOutputFormat::~MongoDBOutputFormat() { records_vector_.clear(); }

void MongoDBOutputFormat::set_ns(const std::string& database, const std::string& collection) {
    database_ = database;
    collection_ = collection;
    ns_ = database_ + "." + collection_;
    is_setup_ |= MongoDBOutputFormatSetUp::NSSetUp;
}

void MongoDBOutputFormat::set_server(std::string server) {
    server_ = server;
    is_setup_ |= MongoDBOutputFormatSetUp::ServerSetUp;
}

void MongoDBOutputFormat::set_auth(const std::string& username, const std::string& password) {
    username_ = username;
    password_ = password;
    need_auth_ = true;
    is_setup_ |= MongoDBOutputFormatSetUp::AuthSetUp;
}

bool MongoDBOutputFormat::is_setup() const { return !(is_setup_ ^ MongoDBOutputFormatSetUp::AllSetUp); }

bool MongoDBOutputFormat::commit(const std::string& doc) {
    if (!is_setup())
        return false;

    if (doc.empty())
        return false;

    mongo::BSONObj obj = mongo::fromjson(doc);
    records_vector_.push_back(obj.copy());

    if (records_vector_.size() >= kMaxNumberOfRecord)
        flush_all();

    return true;
}

void MongoDBOutputFormat::flush_all() {
    if (!is_setup())
        throw husky::base::HuskyException("MongoDBOutputFormat not setup correctly!");

    if (records_vector_.empty())
        return;

    mongo::DBClientConnection client;
    client.connect(server_, error_msg_);

    if (need_auth_)
        client.auth(database_, username_, password_, error_msg_);

    client.insert(ns_, records_vector_);

    error_msg_ = client.getLastError();
    if (!error_msg_.empty()) {
        log_msg(error_msg_, LOG_TYPE::LOG_ERROR);
        return;
    }

    records_vector_.clear();
}

}  // namespace io
}  // namespace husky
