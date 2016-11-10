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

#include "io/input/xml_inputformat.hpp"

#include <string>

#include "boost/utility/string_ref.hpp"

#include "base/exception.hpp"
#include "io/input/inputformat_helper.hpp"

namespace husky {
namespace io {

enum XMLInputFormatSetUp {
    NotSetUp = 0,
    InputSetUp = 1 << 1,
    AllSetUp = InputSetUp,
};

XMLInputFormat::XMLInputFormat(const std::string& start_pattern, const std::string& end_pattern)
    : start_pattern_(start_pattern), end_pattern_(end_pattern) {
    is_setup_ = XMLInputFormatSetUp::NotSetUp;
}

XMLInputFormat::~XMLInputFormat() {
    if (!splitter_)
        return;
    delete splitter_;
    splitter_ = nullptr;
}

void XMLInputFormat::set_input(const std::string& url) {
    set_splitter(url);
    // reset input format
    l = r = 0;
    last_part_ = "";
    buffer_.clear();
    is_setup_ |= XMLInputFormatSetUp::InputSetUp;
}

bool XMLInputFormat::is_setup() const { return !(is_setup_ ^ XMLInputFormatSetUp::AllSetUp); }

bool XMLInputFormat::next(boost::string_ref& ref) {
    if (buffer_.empty()) {
        bool success = fetch_new_block();
        if (!success)
            return false;
    }

    if (r + end_pattern_.size() == buffer_.size()) {
        bool success = fetch_new_block();
        if (!success)
            return false;
    }

    if (r == 0) {
        l = helper::find_next(buffer_, 0, start_pattern_);
    } else {
        l = helper::find_next(buffer_, r + end_pattern_.size(), start_pattern_);
    }

    if (l == boost::string_ref::npos) {
        // store the last start_pattern_.size() bytes
        last_part_ = buffer_.substr(buffer_.size() - start_pattern_.size()).to_string();
        buffer_ = splitter_->fetch_block(true);
        // stores the extracted string directly into last_part_, discarding start and end pattern
        bool found = handle_next_block_start_pattern();
        if (found) {
            ref = last_part_;
            return true;
        } else {
            buffer_.clear();
            return next(ref);
        }
    }
    r = helper::find_next(buffer_, l + start_pattern_.size(), end_pattern_);
    if (r == boost::string_ref::npos) {
        // stores the current string and search the next block for end_pattern_
        last_part_ = buffer_.substr(l + start_pattern_.size()).to_string();
        buffer_ = splitter_->fetch_block(true);
        // stores the extracted string directly into last_part_, discarding start and end pattern
        handle_next_block_end_pattern();
        ref = last_part_;
        return true;
    } else {
        ref = buffer_.substr(l + start_pattern_.size(), r - l - start_pattern_.size());
        return true;
    }
}

bool XMLInputFormat::handle_next_block_start_pattern() {
    if (buffer_.empty()) {
        return false;
    }
    auto pre_last_part_size = last_part_.size();
    // search at most to the start_pattern.size() - 1 position
    last_part_ += buffer_.substr(0, start_pattern_.size() - 1).to_string();
    l = helper::find_next(last_part_, 0, start_pattern_);
    if (l != boost::string_ref::npos) {
        r = helper::find_next(buffer_, l + start_pattern_.size() - pre_last_part_size, end_pattern_);
        if (r == boost::string_ref::npos) {
            throw base::HuskyException("data format error!");
        } else {
            last_part_ = last_part_.substr(l, pre_last_part_size - l) + buffer_.substr(0, r).to_string();
            last_part_ = last_part_.substr(start_pattern_.size());
            clear_buffer();
            return true;
        }
    }
    return false;
}

void XMLInputFormat::handle_next_block_end_pattern() {
    if (buffer_.empty()) {
        throw base::HuskyException("data format error!");
    }
    last_part_ += buffer_.to_string();
    r = helper::find_next(last_part_, 0, end_pattern_);
    if (r == boost::string_ref::npos) {
        throw base::HuskyException("data format error!");
    } else {
        last_part_ = last_part_.substr(0, r);
        clear_buffer();
        return;
    }
}

bool XMLInputFormat::fetch_new_block() {
    buffer_ = splitter_->fetch_block(false);
    if (buffer_.empty())
        return false;
    l = r = 0;
    return true;
}

void XMLInputFormat::clear_buffer() {
    buffer_.clear();
    l = r = 0;
}

}  // namespace io
}  // namespace husky
