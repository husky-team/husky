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

#include "base/session_local.hpp"

namespace husky {
namespace io {

thread_local int InputFormatStore::default_inputformat_id = 0;
thread_local std::unordered_map<std::string, InputFormatBase*> InputFormatStore::inputformat_map;
const char* InputFormatStore::inputformat_name_prefix = "default_inputformat_";

// set finalize_all_inputformats priority to Level1, the higher the level the higher the priorty
static thread_local base::RegSessionThreadFinalizer finalize_all_inputformats(base::SessionLocalPriority::Level1, []() {
    InputFormatStore::drop_all_inputformats();
});

LineInputFormat& InputFormatStore::create_line_inputformat(const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatStore::create_inputformat: Inputformat name already exists");
    auto* line_input_format = new LineInputFormat();
    inputformat_map.insert({inputformat_name, line_input_format});
    return *line_input_format;
}

LineInputFormat& InputFormatStore::get_line_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatStore::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<LineInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr, "InputFormatStore::get_line_inputformat: given name is not of type LineInputFormat");
    return *ret;
}

ChunkInputFormat& InputFormatStore::create_chunk_inputformat(const int& chunk_size, const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatStore::create_inputformat: Inputformat name already exists");
    auto* chunk_input_format = new ChunkInputFormat(chunk_size);
    inputformat_map.insert({inputformat_name, chunk_input_format});
    return *chunk_input_format;
}

ChunkInputFormat& InputFormatStore::get_chunk_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatStore::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<ChunkInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr, "InputFormatStore::get_chunk_inputformat: given name is not of type ChunkInputFormat");
    return *ret;
}

SeparatorInputFormat& InputFormatStore::create_separator_inputformat(const std::string& pattern,
                                                                     const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatStore::create_inputformat: Inputformat name already exists");
    auto* separator_input_format = new SeparatorInputFormat(pattern);
    inputformat_map.insert({inputformat_name, separator_input_format});
    return *separator_input_format;
}

SeparatorInputFormat& InputFormatStore::get_separator_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatStore::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<SeparatorInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr,
               "InputFormatStore::get_separator_inputformat: given name is not of type SeparatorInputFormat");
    return *ret;
}

XMLInputFormat& InputFormatStore::create_xml_inputformat(const std::string& start_pattern,
                                                         const std::string& end_pattern, const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatStore::create_inputformat: Inputformat name already exists");
    auto* xml_input_format = new XMLInputFormat(start_pattern, end_pattern);
    inputformat_map.insert({inputformat_name, xml_input_format});
    return *xml_input_format;
}

XMLInputFormat& InputFormatStore::get_xml_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatStore::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<XMLInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr, "InputFormatStore::get_xml_inputformat: given name is not of type XMLInputFormat");
    return *ret;
}

BinaryInputFormat& InputFormatStore::create_binary_inputformat(const std::string& url, const std::string& filter,
                                                               const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatStore::create_inputformat: Inputformat name already exists");
    auto* binary_input_format = new BinaryInputFormat(url, filter);
    inputformat_map.insert({inputformat_name, binary_input_format});
    return *binary_input_format;
}

BinaryInputFormat& InputFormatStore::get_binary_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatStore::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<BinaryInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr, "InputFormatStore::get_binary_inputformat: given name is not of type BinaryInputFormat");
    return *ret;
}

#ifdef WITH_THRIFT
FlumeInputFormat& InputFormatStore::create_flume_inputformat(std::string rcv_host, int rcv_port,
                                                             const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatStore::create_inputformat: Inputformat name already exists");
    auto* flume_input_format = new FlumeInputFormat(rcv_host, rcv_port);
    inputformat_map.insert({inputformat_name, flume_input_format});
    return *flume_input_format;
}

FlumeInputFormat& InputFormatStore::get_flume_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatStore::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<FlumeInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr, "InputFormatStore::get_flume_inputformat: given name is not of type FlumeInputFormat");
    return *ret;
}
#endif

#ifdef WITH_MONGODB
MongoDBInputFormat& InputFormatStore::create_mongodb_inputformat(const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatStore::create_inputformat: Inputformat name already exists");
    auto* mongodb_input_format = new MongoDBInputFormat();
    inputformat_map.insert({inputformat_name, mongodb_input_format});
    return *mongodb_input_format;
}

MongoDBInputFormat& InputFormatStore::get_mongodb_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatStore::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<MongoDBInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr,
               "InputFormatStore::get_mongodb_inputformat: given name is not of type MongoDBInputFormat");
    return *ret;
}
#endif

void InputFormatStore::drop_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatStore::drop_inputformat: InputFormat name doesn't exist");
    delete inputformat_map[name];
    inputformat_map.erase(name);
}

void InputFormatStore::drop_all_inputformats() {
    for (auto& inputformat_pair : inputformat_map) {
        delete inputformat_pair.second;
    }
    inputformat_map.clear();
}

bool InputFormatStore::has_inputformat(const std::string& name) {
    return inputformat_map.find(name) != inputformat_map.end();
}

}  // namespace io
}  // namespace husky
