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

#include "io/input/inputformat_factory.hpp"

#include <string>
#include <unordered_map>

#include "base/session_local.hpp"

namespace husky {

namespace io {

thread_local int InputFormatFactory::default_inputformat_id = 0;
thread_local std::unordered_map<std::string, InputFormatBase*> InputFormatFactory::inputformat_map;
const char* InputFormatFactory::inputformat_name_prefix = "default_inputformat_";

// set finalize_all_inputformats priority to Level1, the higher the level the higher the priorty
static thread_local base::RegSessionThreadFinalizer finalize_all_inputformats(base::SessionLocalPriority::Level1, []() {
    InputFormatFactory::drop_all_inputformats();
});

LineInputFormat& InputFormatFactory::create_line_inputformat(const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatFactory::create_inputformat: Inputformat name already exists");
    auto* line_input_format = new LineInputFormat();
    inputformat_map.insert({inputformat_name, line_input_format});
    return *line_input_format;
}

LineInputFormat& InputFormatFactory::get_line_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatFactory::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<LineInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr,
               "InputFormatFactory::get_line_inputformat: given name is not of type LineInputFormat");
    return *ret;
}

ChunkInputFormat& InputFormatFactory::create_chunk_inputformat(const int& chunk_size, const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatFactory::create_inputformat: Inputformat name already exists");
    auto* chunk_input_format = new ChunkInputFormat(chunk_size);
    inputformat_map.insert({inputformat_name, chunk_input_format});
    return *chunk_input_format;
}

ChunkInputFormat& InputFormatFactory::get_chunk_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatFactory::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<ChunkInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr,
               "InputFormatFactory::get_chunk_inputformat: given name is not of type ChunkInputFormat");
    return *ret;
}

SeparatorInputFormat& InputFormatFactory::create_separator_inputformat(const std::string& pattern,
                                                                       const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatFactory::create_inputformat: Inputformat name already exists");
    auto* separator_input_format = new SeparatorInputFormat(pattern);
    inputformat_map.insert({inputformat_name, separator_input_format});
    return *separator_input_format;
}

SeparatorInputFormat& InputFormatFactory::get_separator_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatFactory::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<SeparatorInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr,
               "InputFormatFactory::get_separator_inputformat: given name is not of type SeparatorInputFormat");
    return *ret;
}

XMLInputFormat& InputFormatFactory::create_xml_inputformat(const std::string& start_pattern,
                                                           const std::string& end_pattern, const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatFactory::create_inputformat: Inputformat name already exists");
    auto* xml_input_format = new XMLInputFormat(start_pattern, end_pattern);
    inputformat_map.insert({inputformat_name, xml_input_format});
    return *xml_input_format;
}

XMLInputFormat& InputFormatFactory::get_xml_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatFactory::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<XMLInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr,
               "InputFormatFactory::get_xml_inputformat: given name is not of type XMLInputFormat");
    return *ret;
}

BinaryInputFormat& InputFormatFactory::create_binary_inputformat(const std::string& url,
                                                                 const std::string& filter, const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatFactory::create_inputformat: Inputformat name already exists");
    auto* binary_input_format = new BinaryInputFormat(url, filter);
    inputformat_map.insert({inputformat_name, binary_input_format});
    return *binary_input_format;
}

BinaryInputFormat& InputFormatFactory::get_binary_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatFactory::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<BinaryInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr,
               "InputFormatFactory::get_binary_inputformat: given name is not of type BinaryInputFormat");
    return *ret;
}

#ifdef WITH_THRIFT
FlumeInputFormat& InputFormatFactory::create_flume_inputformat(std::string rcv_host, int rcv_port,
                                                               const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatFactory::create_inputformat: Inputformat name already exists");
    auto* flume_input_format = new FlumeInputFormat(rcv_host, rcv_port);
    inputformat_map.insert({inputformat_name, flume_input_format});
    return *flume_input_format;
}

FlumeInputFormat& InputFormatFactory::get_flume_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatFactory::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<FlumeInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr,
               "InputFormatFactory::get_flume_inputformat: given name is not of type FlumeInputFormat");
    return *ret;
}
#endif

#ifdef WITH_MONGODB
MongoDBInputFormat& InputFormatFactory::create_mongodb_inputformat(const std::string& name) {
    std::string inputformat_name =
        name.empty() ? inputformat_name_prefix + std::to_string(default_inputformat_id++) : name;
    ASSERT_MSG(inputformat_map.find(inputformat_name) == inputformat_map.end(),
               "InputFormatFactory::create_inputformat: Inputformat name already exists");
    auto* mongodb_input_format = new MongoDBInputFormat();
    inputformat_map.insert({inputformat_name, mongodb_input_format});
    return *mongodb_input_format;
}

MongoDBInputFormat& InputFormatFactory::get_mongodb_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatFactory::get_inputformat: Inputformat name doesn't exist");
    auto* ret = dynamic_cast<MongoDBInputFormat*>(inputformat_map[name]);
    ASSERT_MSG(ret != nullptr,
               "InputFormatFactory::get_mongodb_inputformat: given name is not of type MongoDBInputFormat");
    return *ret;
}
#endif

void InputFormatFactory::drop_inputformat(const std::string& name) {
    ASSERT_MSG(inputformat_map.find(name) != inputformat_map.end(),
               "InputFormatFactory::drop_inputformat: InputFormat name doesn't exist");
    delete inputformat_map[name];
    inputformat_map.erase(name);
}

void InputFormatFactory::drop_all_inputformats() {
    for (auto& inputformat_pair : inputformat_map) {
        delete inputformat_pair.second;
    }
    inputformat_map.clear();
}

bool InputFormatFactory::has_inputformat(const std::string& name) {
    return inputformat_map.find(name) != inputformat_map.end();
}

}  // namespace io

}  // namespace husky
