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
#include "io/output/outputformat_base.hpp"

namespace husky {
namespace io {

class ElasticsearchOutputFormat final : public OutputFormatBase {
   public:
    typedef std::string RecordT;
    ElasticsearchOutputFormat();
    virtual ~ElasticsearchOutputFormat();
    virtual bool is_setup() const;
    bool is_active();

    bool set_server(const std::string& server, const bool& local_prefer = true);

    bool set_index(const std::string& index, const std::string& type, const std::string& id,
                   const boost::property_tree::ptree& content);

    bool set_index(const std::string& index, const std::string& type, const boost::property_tree::ptree& content);

    bool bulk_add(const std::string& opt, const std::string& index, const std::string& type, const std::string& id,
                  const std::string& content);

    bool bulk_is_full();

    void bulk_flush();

    void bulk_setbound(const int bound) { bound_ = bound; }

   protected:
    bool is_local_prefer_;
    boost::property_tree::ptree result;
    std::string node_;
    std::string node_id;
    std::string server_;
    std::string index_;
    std::string type_;
    std::string id_;
    std::string opt_;
    int bound_;
    boost::property_tree::ptree content_;
    std::string shard_;
    std::string router_;
    std::vector<std::string> records_vector_;
    std::stringstream data;
    HTTP http_conn_;
};

}  // namespace io
}  // namespace husky
