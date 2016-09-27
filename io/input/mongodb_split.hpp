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

#include "base/serialization.hpp"

namespace husky {
namespace io {

using base::BinStream;

class MongoDBSplit {
   public:
    MongoDBSplit();
    MongoDBSplit(const MongoDBSplit& other);

    void set_valid(bool valid);
    void set_input_uri(const std::string& uri);
    void set_max(const std::string& max);
    void set_min(const std::string& min);
    void set_ns(const std::string& ns);

    inline bool is_valid() const { return is_valid_; }
    inline const std::string& get_input_uri() const { return input_uri_; }
    inline const std::string& get_max() const { return max_; }
    inline const std::string& get_min() const { return min_; }
    inline const std::string& get_ns() const { return ns_; }

   private:
    bool is_valid_;
    std::string input_uri_;
    std::string max_;
    std::string min_;
    std::string ns_;
};

BinStream& operator<<(BinStream& stream, MongoDBSplit& split);
BinStream& operator>>(BinStream& stream, MongoDBSplit& split);

}  // namespace io
}  // namespace husky
