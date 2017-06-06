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

#pragma once

#include <string>
#include <vector>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "io/input/elasticsearch_connector/http.h"
#include "io/input/inputformat_base.hpp"

namespace husky {
namespace io {

class ElasticsearchInputFormat final : public InputFormatBase {
   public:
    typedef std::string RecordT;
    ElasticsearchInputFormat();
    virtual ~ElasticsearchInputFormat();
    virtual bool is_setup() const;
    bool is_active();
    int find_shard();

    bool set_server(const std::string& server, bool local_prefer = true,
                    bool local_only = false);

    void set_query(const std::string& index, const std::string& type, const std::string& query);

    bool get_document(const std::string& index, const std::string& type, const std::string& id);

    int scan_fully(const std::string& index, const std::string& type, const std::string& query, int scrollSize = 100);

    virtual bool next(RecordT& ref);

    void read(boost::property_tree::ptree jresult, bool is_clear = true);

   protected:
    bool is_local_prefer_;
    bool is_local_only_;
    boost::property_tree::ptree result;
    std::string node_;
    std::string node_id;
    std::string server_;
    std::string index_;
    std::string type_;
    std::string id_;
    std::string query_;
    std::string router_;
    std::vector<RecordT> records_vector_;
    std::string records_shards_[100];
    HTTP http_conn_;
};

}  // namespace io
}  // namespace husky
