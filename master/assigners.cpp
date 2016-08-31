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

#include "master/assigners.hpp"

#include <fstream>
#include <map>
#include <string>
#include <utility>

#ifdef WITH_HDFS
#include "hdfs/hdfs.h"
#endif

#ifdef WITH_MONGODB
#include "mongo/bson/bson.h"
#include "mongo/client/dbclient.h"
#endif

#include "base/log.hpp"
#include "core/context.hpp"
#include "io/hdfs_manager.hpp"
#include "io/input/mongodb_split.hpp"

namespace husky {

bool operator==(const Triplet& lhs, const Triplet& rhs) {
    return lhs.filename == rhs.filename && lhs.offset == rhs.offset && lhs.block_location == rhs.block_location;
}

#ifdef WITH_HDFS
// HDFSAssigner

HDFSAssigner::~HDFSAssigner() {}

void HDFSAssigner::init_hdfs(const std::string& node, const std::string& port) {
    int num_retries = 3;
    while (num_retries--) {
        struct hdfsBuilder* builder = hdfsNewBuilder();
        hdfsBuilderSetNameNode(builder, node.c_str());
        hdfsBuilderSetNameNodePort(builder, std::stoi(port));
        fs_ = hdfsBuilderConnect(builder);
        hdfsFreeBuilder(builder);
        if (fs_)
            break;
    }
    if (fs_) {
        is_valid_ = true;
        return;
    }
    base::log_msg("Failed to connect to HDFS " + node + ":" + port);
}

void HDFSAssigner::browse_hdfs(const std::string& url) {
    if (!fs_)
        return;

    int num_files;
    int dummy;
    hdfsFileInfo* file_info = hdfsListDirectory(fs_, url.c_str(), &num_files);
    auto& files_locality = files_locality_dict[url];
    for (int i = 0; i < num_files; i++) {
        if (file_info[i].mKind != kObjectKindFile)
            continue;
#ifdef JNI_HDFS
        int replication_factor = 3;
        char*** hosts = hdfsGetHosts(fs_, file_info[i].mName, 0, 1);
        for (int j = 0; j < replication_factor; j++) {
            files_locality.insert(std::make_pair(std::string(file_info[i].mName) + '\0', std::string(hosts[0][j])));
        }
#else
        auto blk_loc = hdfsGetFileBlockLocations(fs_, file_info[i].mName, 0, 1, &dummy);
        for (int j = 0; j < blk_loc->numOfNodes; j++)
            files_locality.insert(
                std::make_pair(std::string(file_info[i].mName) + '\0', std::string(blk_loc->hosts[j])));
#endif
    }
}
std::string HDFSAssigner::answer(const std::string& host, const std::string& url) {
    if (!fs_)
        return "";

    if (files_locality_dict.find(url) == files_locality_dict.end()) {
        browse_hdfs(url);
        finish_dict[url] = 0;
    }

    std::string selected_file = "";
    auto& files_locality = files_locality_dict[url];
    if (files_locality.size() == 0) {
        finish_dict[url] += 1;
        return "";
    }

    for (auto& pair : files_locality)
        if (pair.second == host)
            selected_file = pair.first;

    if (selected_file == "")
        selected_file = files_locality.begin()->first;

    for (auto it = files_locality.begin(); it != files_locality.end();) {
        if (it->first == selected_file)
            it = files_locality.erase(it);
        else
            it++;
    }

    return selected_file;
}

/// Return the number of workers who have finished reading the files in
/// the given url
int HDFSAssigner::get_num_finished(std::string& url) { return finish_dict[url]; }

/// Use this when all workers finish reading the files in url
void HDFSAssigner::finish_url(std::string& url) { files_locality_dict.erase(url); }

// HDFSBlockAssigner
HDFSBlockAssigner::~HDFSBlockAssigner() {}

void HDFSBlockAssigner::init_hdfs(const std::string& node, const std::string& port) {
    int num_retries = 3;
    while (num_retries--) {
        struct hdfsBuilder* builder = hdfsNewBuilder();
        hdfsBuilderSetNameNode(builder, node.c_str());
        hdfsBuilderSetNameNodePort(builder, std::stoi(port));
        fs_ = hdfsBuilderConnect(builder);
        hdfsFreeBuilder(builder);
        if (fs_)
            break;
    }
    if (fs_) {
        is_valid_ = true;
        return;
    }
    base::log_msg("Failed to connect to HDFS " + node + ":" + port);
}

void HDFSBlockAssigner::browse_hdfs(const std::string& url) {
    if (!fs_)
        return;

    int num_files;
    int dummy;
    hdfsFileInfo* file_info = hdfsListDirectory(fs_, url.c_str(), &num_files);
    auto& files_locality = files_locality_dict[url];
    for (int i = 0; i < num_files; ++i) {
        // for every file in a directory
        if (file_info[i].mKind != kObjectKindFile)
            continue;
        size_t k = 0;
        while (k < file_info[i].mSize) {
            // for every block in a file
            auto blk_loc = hdfsGetFileBlockLocations(fs_, file_info[i].mName, k, 1, &dummy);
            for (int j = 0; j < blk_loc->numOfNodes; ++j) {
                // for every replication in a block
                files_locality.insert(
                    Triplet{std::string(file_info[i].mName) + '\0', k, std::string(blk_loc->hosts[j])});
            }
            k += file_info[i].mBlockSize;
        }
    }
    hdfsFreeFileInfo(file_info, num_files);
}

std::pair<std::string, size_t> HDFSBlockAssigner::answer(const std::string& host, const std::string& url) {
    if (!fs_)
        return {"", 0};

    // cannot find url
    if (files_locality_dict.find(url) == files_locality_dict.end()) {
        browse_hdfs(url);
        finish_dict[url] = 0;
    }

    // empty url
    auto& files_locality = files_locality_dict[url];
    if (files_locality.size() == 0) {
        finish_dict[url] += 1;
        if (finish_dict[url] == num_workers_alive)
            files_locality_dict.erase(url);
        return {"", 0};
    }

    std::pair<std::string, size_t> ret = {"", 0};  // selected_file, offset
    for (auto& triplet : files_locality)
        if (triplet.block_location == host) {
            ret = {triplet.filename, triplet.offset};
            break;
        }

    if (ret.first == "")
        ret = {files_locality.begin()->filename, files_locality.begin()->offset};

    // remove
    for (auto it = files_locality.begin(); it != files_locality.end();) {
        if (it->filename == ret.first && it->offset == ret.second)
            it = files_locality.erase(it);
        else
            it++;
    }

    return ret;
}

/// Return the number of workers who have finished reading the files in
/// the given url
int HDFSBlockAssigner::get_num_finished(std::string& url) { return finish_dict[url]; }

/// Use this when all workers finish reading the files in url
void HDFSBlockAssigner::finish_url(std::string& url) { files_locality_dict.erase(url); }
#endif

#ifdef WITH_MONGODB
const char kConfigDB[] = "config";
const char kConfigChunks[] = "config.chunks";
const char kConfigShards[] = "config.shards";

MongoSplitAssigner::MongoSplitAssigner() : end_count_(0), split_num_(0) { mongo::client::initialize(); }

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
#endif

}  // namespace husky
