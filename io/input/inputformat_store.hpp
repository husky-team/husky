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
#include <unordered_map>

#include "io/input/binary_inputformat.hpp"
#include "io/input/chunk_inputformat.hpp"
#ifdef WITH_THRIFT
#include "io/input/flume_inputformat.hpp"
#endif
#include "io/input/line_inputformat.hpp"
#ifdef WITH_MONGODB
#include "io/input/mongodb_inputformat.hpp"
#endif
#include "io/input/separator_inputformat.hpp"
#include "io/input/xml_inputformat.hpp"

namespace husky {
namespace io {

class InputFormatStore {
   public:
    // Create LineInputFormat
    static LineInputFormat& create_line_inputformat(const std::string& name = "");
    static LineInputFormat& get_line_inputformat(const std::string& name = "");

    // Create ChunkInputFormat
    static ChunkInputFormat& create_chunk_inputformat(const int& chunk_size, const std::string& name = "");
    static ChunkInputFormat& get_chunk_inputformat(const std::string& name = "");

    // Create SeparatorInputFormat
    static SeparatorInputFormat& create_separator_inputformat(const std::string& pattern, const std::string& name);
    static SeparatorInputFormat& get_separator_inputformat(const std::string& name = "");

    // Create XMLInputFormat
    static XMLInputFormat& create_xml_inputformat(const std::string& start_pattern, const std::string& end_pattern,
                                                  const std::string& name = "");
    static XMLInputFormat& get_xml_inputformat(const std::string& name = "");

    // Create BinaryInputFormat
    static BinaryInputFormat& create_binary_inputformat(const std::string& url, const std::string& filter = "",
                                                        const std::string& name = "");
    static BinaryInputFormat& get_binary_inputformat(const std::string& name = "");

#ifdef WITH_THRIFT
    // Create FlumeInputFormat
    static FlumeInputFormat& create_flume_inputformat(std::string rcv_host = "localhost", int rcv_port = 2016,
                                                      const std::string& name = "");
    static FlumeInputFormat& get_flume_inputformat(const std::string& name = "");
#endif

#ifdef WITH_MONGODB
    // Create MongoDBInputFormat
    static MongoDBInputFormat& create_mongodb_inputformat(const std::string& name = "");
    static MongoDBInputFormat& get_mongodb_inputformat(const std::string& name = "");
#endif

    static void drop_inputformat(const std::string& name);
    static void drop_all_inputformats();
    static bool has_inputformat(const std::string& name);
    static inline size_t size() { return inputformat_map.size(); }

   protected:
    static thread_local std::unordered_map<std::string, InputFormatBase*> inputformat_map;
    static thread_local int default_inputformat_id;
    static const char* inputformat_name_prefix;
};

}  // namespace io

}  // namespace husky
