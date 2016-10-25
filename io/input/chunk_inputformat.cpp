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

#include "io/input/chunk_inputformat.hpp"

#include <string>

#include "boost/utility/string_ref.hpp"

#include "io/input/inputformat_helper.hpp"

namespace husky {
namespace io {

enum ChunkInputFormatSetUp {
    NotSetUp = 0,
    InputSetUp = 1 << 1,
    AllSetUp = InputSetUp,
};

ChunkInputFormat::ChunkInputFormat(int chunk_size) : chunk_size_(chunk_size) {
    is_setup_ = ChunkInputFormatSetUp::NotSetUp;
}

ChunkInputFormat::~ChunkInputFormat() {
    if (!splitter_)
        return;
    delete splitter_;
    splitter_ = nullptr;
}

void ChunkInputFormat::set_input(const std::string& url) {
    set_splitter(url);
    // reset input format
    l = r = 0;
    last_part_ = "";
    buffer_.clear();
    is_setup_ |= ChunkInputFormatSetUp::InputSetUp;
}

bool ChunkInputFormat::is_setup() const { return !(is_setup_ ^ ChunkInputFormatSetUp::AllSetUp); }

bool ChunkInputFormat::next(boost::string_ref& ref) {
    if (buffer_.size() == 0) {
        // fetch new block
        bool success = fetch_new_block();
        if (!success)
            return false;
    }
    if (r == buffer_.size()) {
        // last character in block
        bool success = fetch_new_block();
        if (!success)
            return false;
    }
    if (splitter_->get_offset() == 0 && r == 0)
        l = 0;
    // new block: deduce offset information from offset
    else if (r == 0)
        l = (chunk_size_ - splitter_->get_offset() % chunk_size_) % chunk_size_;
    else
        l = r;

    r = l + chunk_size_;

    if (r > buffer_.size()) {
        auto last = buffer_.substr(l);
        last_part_ = std::string(last.data(), last.size());
        int remain = r - buffer_.size();
        buffer_ = splitter_->fetch_block(true);
        handle_next_block(remain);
        ref = last_part_;
        return true;
    } else {
        ref = buffer_.substr(l, chunk_size_);
        return true;
    }
}

void ChunkInputFormat::handle_next_block(int remain) {
    r = remain;
    while (true) {
        if (buffer_.empty())
            return;
        if (r > buffer_.size()) {
            r -= buffer_.size();
            last_part_ += std::string(buffer_.data(), buffer_.size());
            buffer_ = splitter_->fetch_block(true);
            continue;
        } else {
            last_part_ += std::string(buffer_.data(), r);
            clear_buffer();
            return;
        }
    }
}

bool ChunkInputFormat::fetch_new_block() {
    buffer_ = splitter_->fetch_block(false);
    if (buffer_.empty())
        return false;
    l = r = 0;
    return true;
}

void ChunkInputFormat::clear_buffer() {
    buffer_.clear();
    l = r = 0;
}

}  // namespace io
}  // namespace husky
