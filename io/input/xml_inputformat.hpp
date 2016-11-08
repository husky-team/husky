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

#include "boost/utility/string_ref.hpp"

#include "io/input/file_inputformat_impl.hpp"

namespace husky {
namespace io {

class XMLInputFormat final : public FileInputFormatImpl {
   public:
    typedef boost::string_ref RecordT;

    explicit XMLInputFormat(const std::string& start_pattern, const std::string& end_pattern);
    virtual ~XMLInputFormat();

    void set_input(const std::string& url);
    bool next(boost::string_ref& ref);
    bool is_setup() const override;

   protected:
    bool handle_next_block_start_pattern();
    void handle_next_block_end_pattern();
    bool fetch_new_block();
    void clear_buffer();

    int l = 0, r = 0;
    std::string last_part_;
    std::string start_pattern_;
    std::string end_pattern_;

    boost::string_ref buffer_;
};

}  // namespace io
}  // namespace husky
