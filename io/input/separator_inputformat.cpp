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

#include "io/input/separator_inputformat.hpp"

#include <string>

#include "boost/utility/string_ref.hpp"

#include "base/exception.hpp"
#include "io/input/inputformat_helper.hpp"

namespace husky {
namespace io {

enum SeparatorInputFormatSetUp {
    NotSetUp = 0,
    InputSetUp = 1 << 1,
    AllSetUp = InputSetUp,
};

SeparatorInputFormat::SeparatorInputFormat(const std::string& pattern) : pattern_(pattern) {
    is_setup_ = SeparatorInputFormatSetUp::NotSetUp;
}

SeparatorInputFormat::~SeparatorInputFormat() {
    if (!splitter_)
        return;
    delete splitter_;
    splitter_ = nullptr;
}

void SeparatorInputFormat::set_input(const std::string& url) {
    set_splitter(url);
    // reset input format
    l = r = 0;
    last_part_ = "";
    buffer_.clear();
    in_between_ = false;
    is_setup_ |= SeparatorInputFormatSetUp::InputSetUp;
}

bool SeparatorInputFormat::is_setup() const { return !(is_setup_ ^ SeparatorInputFormatSetUp::AllSetUp); }

bool SeparatorInputFormat::next(boost::string_ref& ref) {
    if (in_between_) {
        in_between_ = false;
        clear_buffer();
        ref = in_between_str_;
        return true;
    }
    if (buffer_.size() == 0) {
        bool success = fetch_new_block();
        if (!success)
            return false;
    }
    if (r + pattern_.size() == buffer_.size()) {
        buffer_ = splitter_->fetch_block(true);
        if (buffer_.empty()) {
            bool success = fetch_new_block();
            if (!success)
                return false;
        } else {
            last_part_ = "";
            handle_next_block();
            ref = last_part_;
            return true;
        }
    }

    if (splitter_->get_offset() == 0 && r == 0) {
        // begin of a file, start from index 0
        l = 0;
        // for the case file starting with separator
        if (buffer_.substr(l, pattern_.size()) == pattern_)
            l += pattern_.size();
    } else if (r == 0) {
        l = helper::find_next(buffer_, 0, pattern_);
        // can not find first pattern
        if (l == boost::string_ref::npos) {
            throw base::HuskyException("data format error!");
        }
        l += pattern_.size();
    } else {
        l = r + pattern_.size();
    }

    r = helper::find_next(buffer_, l, pattern_);
    if (r == boost::string_ref::npos) {
        auto last = buffer_.substr(l);
        last_part_ = std::string(last.data(), last.size());
        buffer_ = splitter_->fetch_block(true);
        handle_next_block();
        ref = last_part_;
        return true;
    } else {
        ref = buffer_.substr(l, r - l);
        return true;
    }
}

void SeparatorInputFormat::handle_next_block() {
    if (buffer_.empty()) {
        return;
    }
    auto pre_last_part_size = last_part_.size();
    last_part_ += buffer_.to_string();
    r = helper::find_next(last_part_, 0, pattern_);
    // can not find pattern in the new block
    if (r == boost::string_ref::npos) {
        throw base::HuskyException("data format error!");
    }
    if (r < pre_last_part_size) {
        int save_r = r;
        l = r + pattern_.size();
        r = helper::find_next(last_part_, l, pattern_);
        // can not find pattern after in between pattern in the new block
        if (r == boost::string_ref::npos) {
            throw base::HuskyException("data format error!");
        }
        in_between_ = true;
        in_between_str_ = last_part_.substr(l, r - l);
        last_part_ = last_part_.substr(0, save_r);
        return;
    } else {
        last_part_ = last_part_.substr(0, r);
        clear_buffer();
        return;
    }
}

bool SeparatorInputFormat::fetch_new_block() {
    buffer_ = splitter_->fetch_block(false);
    if (buffer_.empty())
        return false;
    l = r = 0;
    return true;
}

void SeparatorInputFormat::clear_buffer() {
    buffer_.clear();
    l = r = 0;
}

}  // namespace io
}  // namespace husky
