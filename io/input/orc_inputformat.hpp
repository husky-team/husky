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

#include <cassert>
#include <fstream>
#include <string>

#include "boost/utility/string_ref.hpp"

#include "io/input/inputformat_base.hpp"
#include "io/input/orc_file_splitter.hpp"

namespace husky {
namespace io {

class ORCInputFormat : public InputFormatBase {
   public:
    typedef boost::string_ref RecordT;

    ORCInputFormat();
    virtual ~ORCInputFormat() = default;

    virtual void set_input(const std::string& url);
    virtual bool next(boost::string_ref& ref);
    virtual bool is_setup() const;

   protected:
    bool fetch_new_batch();
    void clear_buffer();
    std::string url_;

    int l = 0;
    int r = 0;
    boost::string_ref buffer_;
    ORCFileSplitter splitter_;
};

}  // namespace io
}  // namespace husky
