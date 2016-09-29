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

#include "io/input/nfs_line_inputformat.hpp"

#include <string>

#include "boost/utility/string_ref.hpp"

#include "io/input/inputformat_helper.hpp"
#include "io/input/nfs_file_splitter.hpp"

namespace husky {
namespace io {

enum NFSLineInputFormatSetUp {
    NotSetUp = 0,
    InputSetUp = 1 << 1,
    AllSetUp = InputSetUp,
};

NFSLineInputFormat::NFSLineInputFormat() { is_setup_ = NFSLineInputFormatSetUp::NotSetUp; }

void NFSLineInputFormat::set_input(const std::string& url) {
    splitter_.load(url);
    is_setup_ |= NFSLineInputFormatSetUp::InputSetUp;
}

bool NFSLineInputFormat::is_setup() const { return !(is_setup_ ^ NFSLineInputFormatSetUp::AllSetUp); }

bool NFSLineInputFormat::next(boost::string_ref& ref) {
    if (buffer_.size() == 0) {
        bool success = fetch_new_block();
        if (success == false)
            return false;
    }
    // last charater in block
    if (r == buffer_.size() - 1) {
        // fetch next block
        buffer_ = splitter_.fetch_block(true);
        if (buffer_.empty()) {
            // end of a file
            bool success = fetch_new_block();
            if (success == false)
                return false;
        } else {
            // directly process the remaing
            last_part_ = "";
            handle_next_block();
            ref = last_part_;
            return true;
        }
    }

    if (splitter_.get_offset() == 0 && r == 0) {
        // begin of a file
        l = 0;
        if (buffer_[0] == '\n')
            // for the case file starting with '\n'
            l = 1;
        r = helper::find_next(buffer_, l, '\n');
    } else {
        // what if not found
        l = helper::find_next(buffer_, r, '\n') + 1;
        r = helper::find_next(buffer_, l, '\n');
    }
    // if the right end does not exist in current block
    if (r == boost::string_ref::npos) {
        auto last = buffer_.substr(l);
        last_part_ = std::string(last.data(), last.size());
        // fetch next subBlock
        buffer_ = splitter_.fetch_block(true);
        handle_next_block();
        ref = last_part_;
        return true;
    } else {
        ref = buffer_.substr(l, r - l);
        return true;
    }
}

void NFSLineInputFormat::handle_next_block() {
    while (true) {
        if (buffer_.empty())
            return;
        r = helper::find_next(buffer_, 0, '\n');
        if (r == boost::string_ref::npos) {
            // fetch next subBlock
            last_part_ += std::string(buffer_.data(), buffer_.size());
            buffer_ = splitter_.fetch_block(true);
            continue;
        } else {
            last_part_ += std::string(buffer_.substr(0, r).data(), r);
            clear_buffer();
            return;
        }
    }
}

bool NFSLineInputFormat::fetch_new_block() {
    // fetch a new block
    buffer_ = splitter_.fetch_block(false);
    if (buffer_.empty())
        //  no more files, exit
        return false;
    l = r = 0;
    return true;
}

void NFSLineInputFormat::clear_buffer() {
    buffer_.clear();
    l = r = 0;
}

}  // namespace io
}  // namespace husky
