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

#include "io/input/binary_inputformat_impl.hpp"

#include <algorithm>
#include <string>
#include <vector>

#include "base/serialization.hpp"
#include "core/utils.hpp"
#include "io/input/inputformat_base.hpp"

namespace husky {
namespace io {

const size_t FileBinStreamBase::DEFAULT_BUF_SIZE = 1024 * 1024;  // 1MB

FileBinStreamBase::FileBinStreamBase(const std::string& file) : filename_(file) {}

const std::string& FileBinStreamBase::get_filename() { return filename_; }

void* FileBinStreamBase::pop_front_bytes(size_t sz) {
    auto cur_size = BinStream::size();
    ASSERT_MSG(sz <= size(), ("Trying to extract more bytes than avaiable. Extract: " + std::to_string(sz) +
                              ", Available: " + std::to_string(size()) + ". File: " + get_filename())
                                 .c_str());
    if (cur_size < sz) {                  // not enough
        if (sz > this->buffer_.size()) {  // should enlarge
            std::vector<char> new_buffer(std::min(size(), std::max(sz, DEFAULT_BUF_SIZE)));
            memcpy(&new_buffer[0], get_remained_buffer(), cur_size);
            read(&new_buffer[cur_size], new_buffer.size() - cur_size);
            new_buffer.swap(this->buffer_);
            BinStream::seek(0);
        } else {  // no need to enlarge
            auto move_len = std::min(this->buffer_.size(), size()) - cur_size;
            char* cur_buf = const_cast<char*>(get_remained_buffer());
            memmove(cur_buf - move_len, cur_buf, cur_size);
            read(cur_buf - move_len + cur_size, move_len);
            BinStream::seek(this->buffer_.size() - move_len - cur_size);
        }
    }
    return BinStream::pop_front_bytes(sz);
}

void BinaryInputFormatRecord::set_bin_stream(FileBinStreamBase* b) {
    remove_if_not_null();
    bin_ = b;
}

BinaryInputFormatRecord::~BinaryInputFormatRecord() { remove_if_not_null(); }

BinaryInputFormatRecord::CastRecordT& BinaryInputFormatRecord::cast() {
    ASSERT_MSG(bin_ != nullptr, "Cannot casting null FileBinStreamBase pointor to BinStream");
    return *bin_;
}

void BinaryInputFormatRecord::remove_if_not_null() {
    if (bin_ != nullptr) {
        delete bin_;
        bin_ = nullptr;
    }
}

// Should not called by user
bool BinaryInputFormatImpl::is_setup() const { return this->is_setup_ == BinaryInputFormatImplSetUp::AllSetUp; }

BinaryInputFormatImpl::BinaryInputFormatImpl() { this->is_setup_ = BinaryInputFormatImplSetUp::NotSetUp; }

void BinaryInputFormatImpl::to_be_setup() { this->is_setup_ |= BinaryInputFormatImplSetUp::AllSetUp; }

}  // namespace io
}  // namespace husky
