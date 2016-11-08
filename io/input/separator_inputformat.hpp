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

class SeparatorInputFormat final : public FileInputFormatImpl {
   public:
    typedef boost::string_ref RecordT;

    explicit SeparatorInputFormat(const std::string& pattern);
    virtual ~SeparatorInputFormat();

    void set_input(const std::string& url);
    bool next(boost::string_ref& ref);
    bool is_setup() const override;

   protected:
    void handle_next_block();
    bool fetch_new_block();
    void clear_buffer();

    int l = 0, r = 0;
    std::string last_part_;
    std::string pattern_;

    bool in_between_ = false;
    boost::string_ref buffer_;
    std::string in_between_str_;
};

}  // namespace io
}  // namespace husky
