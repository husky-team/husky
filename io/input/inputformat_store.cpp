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

#include "io/input/inputformat_store.hpp"

#include <string>
#include <unordered_map>

#include "base/assert.hpp"
#include "base/session_local.hpp"

namespace husky {
namespace io {

thread_local int g_gen_inputformat_id = 0;
thread_local InputFormatMap* InputFormatStore::s_inputformat_map = nullptr;

// set finalize_all_inputformats priority to Level1, the higher the level the higher the priorty
static thread_local base::RegSessionThreadFinalizer finalize_all_inputformats(base::SessionLocalPriority::Level1, []() {
    InputFormatStore::drop_all_inputformats();
    InputFormatStore::free_inputformat_map();
});

LineInputFormat& InputFormatStore::create_line_inputformat() {
    InputFormatMap& inputformat_map = get_inputformat_map();
    int id = g_gen_inputformat_id++;
    ASSERT_MSG(inputformat_map.find(id) == inputformat_map.end(), "Should not be reached");
    auto* line_input_format = new LineInputFormat();
    inputformat_map.insert({id, line_input_format});
    return *line_input_format;
}

ChunkInputFormat& InputFormatStore::create_chunk_inputformat(const int& chunk_size) {
    InputFormatMap& inputformat_map = get_inputformat_map();
    int id = g_gen_inputformat_id++;
    ASSERT_MSG(inputformat_map.find(id) == inputformat_map.end(), "Should not be reached");
    auto* chunk_input_format = new ChunkInputFormat(chunk_size);
    inputformat_map.insert({id, chunk_input_format});
    return *chunk_input_format;
}

SeparatorInputFormat& InputFormatStore::create_separator_inputformat(const std::string& pattern) {
    InputFormatMap& inputformat_map = get_inputformat_map();
    int id = g_gen_inputformat_id++;
    ASSERT_MSG(inputformat_map.find(id) == inputformat_map.end(), "Should not be reached");
    auto* separator_input_format = new SeparatorInputFormat(pattern);
    inputformat_map.insert({id, separator_input_format});
    return *separator_input_format;
}

XMLInputFormat& InputFormatStore::create_xml_inputformat(const std::string& start_pattern,
                                                         const std::string& end_pattern) {
    InputFormatMap& inputformat_map = get_inputformat_map();
    int id = g_gen_inputformat_id++;
    ASSERT_MSG(inputformat_map.find(id) == inputformat_map.end(), "Should not be reached");
    auto* xml_input_format = new XMLInputFormat(start_pattern, end_pattern);
    inputformat_map.insert({id, xml_input_format});
    return *xml_input_format;
}

BinaryInputFormat& InputFormatStore::create_binary_inputformat(const std::string& url, const std::string& filter) {
    InputFormatMap& inputformat_map = get_inputformat_map();
    int id = g_gen_inputformat_id++;
    ASSERT_MSG(inputformat_map.find(id) == inputformat_map.end(), "Should not be reached");
    auto* binary_input_format = new BinaryInputFormat(url, filter);
    inputformat_map.insert({id, binary_input_format});
    return *binary_input_format;
}

#ifdef WITH_ORC
ORCInputFormat& InputFormatStore::create_orc_inputformat() {
    InputFormatMap& inputformat_map = get_inputformat_map();
    int id = g_gen_inputformat_id++;
    ASSERT_MSG(inputformat_map.find(id) == inputformat_map.end(), "Should not be reached");
    auto* orc_input_format = new ORCInputFormat();
    inputformat_map.insert({id, orc_input_format});
    return *orc_input_format;
}
#endif

#ifdef WITH_THRIFT
FlumeInputFormat& InputFormatStore::create_flume_inputformat(std::string rcv_host, int rcv_port) {
    InputFormatMap& inputformat_map = get_inputformat_map();
    int id = g_gen_inputformat_id++;
    ASSERT_MSG(inputformat_map.find(id) == inputformat_map.end(), "Should not be reached");
    auto* flume_input_format = new FlumeInputFormat(rcv_host, rcv_port);
    inputformat_map.insert({id, flume_input_format});
    return *flume_input_format;
}
#endif

#ifdef WITH_MONGODB
MongoDBInputFormat& InputFormatStore::create_mongodb_inputformat() {
    InputFormatMap& inputformat_map = get_inputformat_map();
    int id = g_gen_inputformat_id++;
    ASSERT_MSG(inputformat_map.find(id) == inputformat_map.end(), "Should not be reached");
    auto* mongodb_input_format = new MongoDBInputFormat();
    inputformat_map.insert({id, mongodb_input_format});
    return *mongodb_input_format;
}
#endif

void InputFormatStore::drop_all_inputformats() {
    if (s_inputformat_map == nullptr)
        return;

    for (auto& inputformat_pair : (*s_inputformat_map)) {
        if (inputformat_pair.second != nullptr)
            delete inputformat_pair.second;
    }
    s_inputformat_map->clear();
}

void InputFormatStore::init_inputformat_map() {
    if (s_inputformat_map == nullptr)
        s_inputformat_map = new InputFormatMap();
}

void InputFormatStore::free_inputformat_map() {
    delete s_inputformat_map;
    s_inputformat_map = nullptr;
}

InputFormatMap& InputFormatStore::get_inputformat_map() {
    init_inputformat_map();
    return *s_inputformat_map;
}

}  // namespace io
}  // namespace husky
