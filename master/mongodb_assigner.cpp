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

#ifdef WITH_MONGODB

#include <utility>
#include <string>

#include "mongo/bson/bson.h"
#include "mongo/client/dbclient.h"

#include "core/constants.hpp"
#include "core/zmq_helpers.hpp"
#include "master/master.hpp"
#include "master/mongodb_assigner.hpp"

namespace husky {

const char kConfigDB[] = "config";
const char kConfigChunks[] = "config.chunks";
const char kConfigShards[] = "config.shards";
static MongoSplitAssigner mongo_split_assigner;

MongoSplitAssigner::MongoSplitAssigner() : end_count_(0), split_num_(0) {
    mongo::client::initialize();
    Master::get_instance().register_main_handler(TYPE_MONGODB_REQ, std::bind(&MongoSplitAssigner::master_mongodb_req_handler, this));
    Master::get_instance().register_main_handler(TYPE_MONGODB_END_REQ, std::bind(&MongoSplitAssigner::master_mongodb_req_end_handler, this));
    Master::get_instance().register_setup_handler(std::bind(&MongoSplitAssigner::master_setup_handler, this));
}

void MongoSplitAssigner::master_mongodb_req_handler() {
    auto& master = Master::get_instance();
    auto* resp_socket = master.get_socket();
    std::string server, ns, host, username, password;
    BinStream stream = zmq_recv_binstream(resp_socket);
    bool need_auth = false;
    stream >> need_auth;
    if (need_auth) {
        stream >> server >> ns >> username >> password >> host;
        set_auth(username, password);
    } else {
        stream >> server >> ns >> host;
        reset_auth();
    }
    MongoDBSplit ret = answer(server, ns);
    stream.clear();
    stream << ret;

    zmq_sendmore_string(resp_socket, master.get_cur_client());
    zmq_sendmore_dummy(resp_socket);
    zmq_send_binstream(resp_socket, stream);
    base::log_msg(host + " => " + ret.get_ns() + " " + ret.get_min() + ":" + ret.get_max());
}

void MongoSplitAssigner::master_mongodb_req_end_handler() {
    auto& master = Master::get_instance();
    auto* resp_socket = master.get_socket();
    BinStream stream = zmq_recv_binstream(resp_socket);
    MongoDBSplit split;
    stream >> split;
    recieve_end(split);

    stream.clear();
    zmq_sendmore_string(resp_socket, master.get_cur_client());
    zmq_sendmore_dummy(resp_socket);
    zmq_send_binstream(resp_socket, stream);
    base::log_msg("master => end@" + split.get_ns() + " " + split.get_min() + ":" + split.get_max());
}

void MongoSplitAssigner::master_setup_handler() {
}

MongoSplitAssigner::~MongoSplitAssigner() {
    shards_map_.clear();
    splits_.clear();
    splits_end_.clear();
}

void MongoSplitAssigner::set_auth(const std::string& username, const std::string& password) {
    username_ = username;
    password_ = password;
    need_auth_ = true;
}

void MongoSplitAssigner::reset_auth() { need_auth_ = false; }

void MongoSplitAssigner::create_splits() {
    mongo::DBClientConnection client;
    client.connect(server_, error_msg_);

    if (need_auth_)
        client.auth(kConfigDB, username_, password_, error_msg_);

    auto shard_cursor = client.query(kConfigShards);
    while (shard_cursor->more()) {
        mongo::BSONObj o = shard_cursor->next();
        std::string shard_id = o.getStringField("_id");
        std::string shard_host = o.getStringField("host");
        shards_map_.insert(std::pair<std::string, std::string>(shard_id, shard_host));
    }

    mongo::BSONObjBuilder ns_filter;
    ns_filter.append("ns", ns_);
    auto chunk_cursor = client.query(kConfigChunks, ns_filter.obj());
    while (chunk_cursor->more()) {
        mongo::BSONObj o = chunk_cursor->next();

        MongoDBSplit split;
        split.set_ns(ns_);

        std::string shard = o.getStringField("shard");
        std::string shard_uri = shards_map_.find(shard)->second;
        split.set_input_uri(shard_uri);

        split.set_min(o.getObjectField("min").jsonString());
        split.set_max(o.getObjectField("max").jsonString());

        split.set_valid(true);
        splits_.push_back(split);
    }
    split_num_ = splits_.size();
}

MongoDBSplit MongoSplitAssigner::answer(const std::string& server, const std::string& ns) {
    if (server_ != server || ns_ != ns) {
        server_ = server;
        ns_ = ns;
        create_splits();
    }

    if (splits_.empty()) {
        if (end_count_ == split_num_) {
            end_count_ = 0;
            splits_.swap(splits_end_);
            splits_end_.clear();
        }
        return MongoDBSplit();
    }

    MongoDBSplit ret = splits_.back();
    splits_.pop_back();
    return ret;
}

void MongoSplitAssigner::recieve_end(MongoDBSplit& split) {
    if (!split.is_valid())
        return;

    end_count_++;
    splits_end_.push_back(split);
}

}  // namespace Husky

#endif
