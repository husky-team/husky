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

#include "io/input/mongodb_split.hpp"

#include <string>

#include "base/serialization.hpp"

namespace husky {
namespace io {

MongoDBSplit::MongoDBSplit() : is_valid_(false) {}

MongoDBSplit::MongoDBSplit(const MongoDBSplit& other) {
    is_valid_ = other.is_valid_;
    max_ = other.max_;
    min_ = other.min_;
    input_uri_ = other.input_uri_;
    ns_ = other.ns_;
}

void MongoDBSplit::set_valid(bool valid) { is_valid_ = valid; }

void MongoDBSplit::set_max(const std::string& max) { max_ = max; }

void MongoDBSplit::set_min(const std::string& min) { min_ = min; }

void MongoDBSplit::set_input_uri(const std::string& uri) { input_uri_ = uri; }

void MongoDBSplit::set_ns(const std::string& ns) { ns_ = ns; }

BinStream& operator<<(BinStream& stream, MongoDBSplit& split) {
    stream << split.is_valid() << split.get_input_uri() << split.get_ns() << split.get_min() << split.get_max();
    return stream;
}

BinStream& operator>>(BinStream& stream, MongoDBSplit& split) {
    bool is_valid;
    std::string input_uri;
    std::string max;
    std::string min;
    std::string ns;
    stream >> is_valid >> input_uri >> ns >> min >> max;
    split.set_valid(is_valid);
    split.set_input_uri(input_uri);
    split.set_ns(ns);
    split.set_max(max);
    split.set_min(min);
    return stream;
}

}  // namespace io
}  // namespace husky
