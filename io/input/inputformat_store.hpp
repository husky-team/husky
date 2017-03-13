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
#ifdef WITH_ORC
#include "io/input/orc_inputformat.hpp"
#endif

namespace husky {
namespace io {

typedef std::unordered_map<int, InputFormatBase*> InputFormatMap;

class InputFormatStore {
   public:
    static LineInputFormat& create_line_inputformat();
    static ChunkInputFormat& create_chunk_inputformat(const int& chunk_size);
    static SeparatorInputFormat& create_separator_inputformat(const std::string& pattern);
    static XMLInputFormat& create_xml_inputformat(const std::string& start_pattern, const std::string& end_pattern);
    static BinaryInputFormat& create_binary_inputformat(const std::string& url, const std::string& filter = "");
#ifdef WITH_ORC
    static ORCInputFormat& create_orc_inputformat();
#endif
#ifdef WITH_THRIFT
    static FlumeInputFormat& create_flume_inputformat(std::string rcv_host, int rcv_port);
#endif
#ifdef WITH_MONGODB
    static MongoDBInputFormat& create_mongodb_inputformat();
#endif

    static void drop_all_inputformats();
    static void init_inputformat_map();
    static void free_inputformat_map();
    static InputFormatMap& get_inputformat_map();

    static inline size_t size() {
        if (s_inputformat_map == nullptr)
            return 0;
        return s_inputformat_map->size();
    }

   protected:
    static thread_local InputFormatMap* s_inputformat_map;
};

}  // namespace io

}  // namespace husky
