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

#include <cstring>
#include <iostream>
#include <list>
#include <sstream>
#include <string>
#include <vector>

#include "base/exception.hpp"
#include "base/serialization.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/coordinator.hpp"
#include "io/input/elasticsearch_connector/http.h"
#include "io/input/elasticsearch_inputformat.hpp"

namespace husky {
namespace io {

enum ElasticsearchInputFormatSetUp {
    NotSetUp = 0,
    ServerSetUp = 1 << 1,
    AllSetUp = ServerSetUp,
};

ElasticsearchInputFormat::ElasticsearchInputFormat() {
    records_vector_.clear();
    is_setup_ = ElasticsearchInputFormatSetUp::NotSetUp;
}

bool ElasticsearchInputFormat::set_server(const std::string& server, bool local_prefer, bool local_only) {
    is_local_prefer_ = local_prefer;
    server_ = server;
    std::size_t found = server_.find(":");
    std::string port = server.substr(found + 1);
    is_local_only_ = local_only;
    if (local_only) {
        server_ = husky::Context::get_worker_info().get_hostname(husky::Context::get_worker_info().get_process_id()) +
                  ":" + port;
        http_conn_.set_url(server_, true);
        if (!is_active())
            throw base::HuskyException("local only mode failed, please set local_only = false");
        is_setup_ = ElasticsearchInputFormatSetUp::AllSetUp;
    } else {
        if (is_local_prefer_) {
            server_ =
                husky::Context::get_worker_info().get_hostname(husky::Context::get_worker_info().get_process_id()) +
                ":" + port;
            http_conn_.set_url(server_, true);
            if (!is_active()) {
                is_local_prefer_ = false;
                server_ = server;
                http_conn_.set_url(server_, true);
                husky::LOG_I << "local engine fail, reconnect with remote engine";
                if (!is_active())
                    throw base::HuskyException(
                        "Cannot create local engine, database is not active and try the remote engine");
                husky::LOG_I << "reconnect successfully";
            }
            is_setup_ = ElasticsearchInputFormatSetUp::AllSetUp;
        } else {
            server_ = server;
            http_conn_.set_url(server_, true);
            if (!is_active())
                throw base::HuskyException("Cannot connect to server");
            is_setup_ = ElasticsearchInputFormatSetUp::AllSetUp;
        }
    }
    // geting the local node_id from the elasticsearch
    std::ostringstream oss;
    oss << "_nodes/_local";
    boost::property_tree::ptree msg;
    http_conn_.get(oss.str().c_str(), 0, &msg);
    node_id = msg.get_child("main").get_child("nodes").begin()->first;

    return true;
}

ElasticsearchInputFormat::~ElasticsearchInputFormat() { records_vector_.clear(); }

bool ElasticsearchInputFormat::is_setup() const { return !(is_setup_ ^ ElasticsearchInputFormatSetUp::AllSetUp); }

bool ElasticsearchInputFormat::is_active() {
    boost::property_tree::ptree root;
    try {
        http_conn_.get(0, 0, &root);
    } catch (Exception& e) {
        husky::LOG_I << "get(0) failed in ElasticsearchInputformat::is_active(). Exception caught: %s\n" << e.what();
        return false;
    } catch (std::exception& e) {
        husky::LOG_I << "get(0) failed in ElasticsearchInputFormat::is_active(). std::exception caught: %s\n"
                     << e.what();
        return false;
    } catch (...) {
        husky::LOG_I << "get(0) failed in ElasticsearchInputFormat::is_active().\n";
        return false;
    }

    if (root.empty())
        return false;

    if (root.get<int>("status") != 200) {
        husky::LOG_I << "Status is not 200. Cannot find Elasticsearch Node.\n";
        return false;
    }

    return true;
}

int ElasticsearchInputFormat::find_shard() {
    std::stringstream url;
    url << index_ << "/_search_shards?";
    boost::property_tree::ptree obj;
    http_conn_.get(url.str().c_str(), 0, &obj);
    boost::property_tree::ptree arr = obj.get_child("main").get_child("shards");
    int shard_num = 0;
    for (auto it = arr.begin(); it != arr.end(); ++it) {
        auto iter(it->second);
        for (auto it_(iter.begin()); it_ != (iter).end(); ++it_) {
            boost::property_tree::ptree obj_ = (it_->second);
            std::string _node = obj_.get<std::string>("node");
            std::string _pri = obj_.get<std::string>("primary");
            if (_node == node_id && _pri == "true") {
                records_shards_[shard_num] = obj_.get<std::string>("shard");
                shard_num++;
            }
        }
    }
    return shard_num;
}

void ElasticsearchInputFormat::set_query(const std::string& index, const std::string& type, const std::string& query) {
    index_ = index;
    type_ = type;
    query_ = query;
    records_vector_.clear();
    while (true) {
        std::string shard_;
        if (is_local_only_) {
            int shard_num = find_shard();
            int worker_num = husky::Context::get_num_local_workers();
            int id = husky::Context::get_local_tid();
            if (id >= shard_num)
                return;
            shard_ = records_shards_[id];
            while (id + worker_num < shard_num) {
                id = id + worker_num;
                shard_ = shard_ + "," + records_shards_[id];
            }
        } else {
            BinStream question;
            question << server_ << index_ << node_id;
            BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_ELASTICSEARCH_REQ);
            answer >> shard_;
        }
        if (shard_ == "No shard")
            return;
        std::stringstream url;
        url << index_ << "/" << type_ << "/_search?preference=_shards:" << shard_;
        http_conn_.post(url.str().c_str(), query_.c_str(), &result);
        if (result.get_child("main").empty()) {
            std::cout << url.str() << " -d " << query << std::endl;
            throw base::HuskyException("Search failed.");
        }
        if (result.get_child("main").get<bool>("timed_out")) {
            throw base::HuskyException("Search timed out.");
        }
        ElasticsearchInputFormat::read(result, false);
        if (is_local_only_) break;
    }
}

bool ElasticsearchInputFormat::get_document(const std::string& index, const std::string& type, const std::string& id) {
    index_ = index;
    type_ = type;
    id_ = id;
    std::stringstream url;
    url << index_ << "/" << type_ << "/" << id_;
    http_conn_.get(url.str().c_str(), 0, &result);
    boost::property_tree::ptree pt = result.get_child("main");
    std::stringstream ss;
    write_json(ss, pt);
    records_vector_.clear();
    records_vector_.push_back(ss.str());
    return pt.get<bool>("found");
}

int ElasticsearchInputFormat::scan_fully(const std::string& index, const std::string& type, const std::string& query,
                                         int scrollSize) {
    index_ = index;
    type_ = type;
    query_ = query;
    records_vector_.clear();
    bool is_first = true;
    while (true) {
        std::string shard_;

        if (is_local_only_) {
            int shard_num = find_shard();
            int worker_num = husky::Context::get_num_local_workers();
            int id = husky::Context::get_local_tid();
            if (id >= shard_num)
                return 0;
            shard_ = records_shards_[id];
            while (id + worker_num < shard_num) {
                id = id + worker_num;
                shard_ = shard_ + "," + records_shards_[id];
            }
        } else {
            BinStream question;
            question << server_ << index_ << node_id;
            BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_ELASTICSEARCH_REQ);
            answer >> shard_;
        }
        if (shard_ == "No shard")
            break;
        std::stringstream scrollUrl;
        scrollUrl << index << "/" << type << "/_search?preference=_shards:" << shard_
                  << "&search_type=scan&scroll=10m&size=" << scrollSize;
        boost::property_tree::ptree scrollObject;
        http_conn_.post(scrollUrl.str().c_str(), query_.c_str(), &scrollObject);
        if (scrollObject.get_child("main").get_child("hits").empty())
            EXCEPTION("Result corrupted, no member \"hits\".");
        if (!scrollObject.get_child("main").get_child("hits").get<int>("total"))
            EXCEPTION("Result corrupted, no member \"total\" nested in \"hits\".");
        int total = scrollObject.get_child("main").get_child("hits").get<int>("total");
        std::string scrollId = scrollObject.get_child("main").get<std::string>("_scroll_id");
        int count = 0;
        while (count < total) {
            boost::property_tree::ptree result;
            http_conn_.rawpost("_search/scroll?scroll=10m", scrollId.c_str(), &result);
            scrollId = result.get_child("main").get<std::string>("_scroll_id");
            read(result, false);
            for (auto it = result.get_child("main").get_child("hits").get_child("hits").begin();
                 it != result.get_child("main").get_child("hits").get_child("hits").end(); ++it)
                ++count;
        }
        if (count != total)
            throw base::HuskyException("Result corrupted, total is different from count.");
        if (is_local_only_)
            break;
    }
    return 0;
}

void ElasticsearchInputFormat::read(boost::property_tree::ptree jresult, bool is_clear) {
    if (!records_vector_.empty() && is_clear)
        records_vector_.clear();
    boost::property_tree::ptree pt = jresult.get_child("main").get_child("hits").get_child("hits");
    for (auto it = pt.begin(); it != pt.end(); ++it) {
        std::stringstream ss;
        write_json(ss, it->second);
        records_vector_.push_back(ss.str());
    }
}

bool ElasticsearchInputFormat::next(RecordT& ref) {
    if (!records_vector_.empty()) {
        ref = records_vector_.back();
        records_vector_.pop_back();
        return true;
    }
    return false;
}

}  // namespace io
}  // namespace husky
