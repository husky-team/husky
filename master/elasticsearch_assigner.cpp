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

#include "master/elasticsearch_assigner.hpp"

#include <string>
#include <utility>

#include "base/log.hpp"
#include "boost/property_tree/ptree.hpp"
#include "core/constants.hpp"
#include "core/zmq_helpers.hpp"
#include "io/input/elasticsearch_connector/http.h"
#include "master/master.hpp"

namespace husky {

static ESShardAssigner es_shard_assigner;

ESShardAssigner::ESShardAssigner() {
    Master::get_instance().register_main_handler(TYPE_ELASTICSEARCH_REQ,
                                                 std::bind(&ESShardAssigner::master_elasticsearch_req_handler, this));
}

void ESShardAssigner::master_elasticsearch_req_handler() {
    auto& master = Master::get_instance();
    auto master_socket = master.get_socket();
    std::string server, index, node;
    BinStream stream = zmq_recv_binstream(master_socket.get());
    stream >> server >> index >> node;
    std::string ret = answer(server, index, node);
    stream.clear();
    stream << ret;

    zmq_sendmore_string(master_socket.get(), master.get_cur_client());
    zmq_sendmore_dummy(master_socket.get());
    zmq_send_binstream(master_socket.get(), stream);
    LOG_I << server << " => " << ret;
}

ESShardAssigner::~ESShardAssigner() {
    shards_.clear();
    nodes_.clear();
}

void ESShardAssigner::create_shards() {
    shards_.clear();
    nodes_.clear();
    HTTP http_conn_;
    http_conn_.set_url(server_, false);
    std::stringstream url;
    url << index_ << "/_search_shards?";
    boost::property_tree::ptree obj;
    http_conn_.get(url.str().c_str(), 0, &obj);
    boost::property_tree::ptree arr = obj.get_child("main").get_child("shards");
    for (auto it = arr.begin(); it != arr.end(); ++it) {
        auto iter(it->second);
        for (auto it_(iter.begin()); it_ != (iter).end(); ++it_) {
            boost::property_tree::ptree obj_ = (it_->second);
            std::string _node = obj_.get<std::string>("node");
            std::string _pri = obj_.get<std::string>("primary");
            if (_pri == "true") {
                nodes_.push_back(_node);
                shards_.push_back(obj_.get<std::string>("shard"));
            }
            shard_num_ = shards_.size();
        }
    }
}

std::string ESShardAssigner::answer(const std::string& server, const std::string& index, std::string node) {
    if (server_ != server && index_ != index) {
        server_ = server;
        index_ = index;
        if (server_ == "reset server")
            return "";
        create_shards();
    }

    if (shards_.empty()) {
        return "No shard";
    }

    int size = shards_.size();
    for (int i = 1; i <= size; i++) {
        std::string nid = nodes_.front();
        nodes_.pop_front();
        std::string ret = shards_.front();
        shards_.pop_front();
        if (nid == node || i == size) {
            LOG_I << i << " ";
            return ret;

            break;
        } else {
            nodes_.push_back(nid);
            shards_.push_back(ret);
        }
    }
}

}  // namespace husky
