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

#include <memory>
#include <string>

#include "io/input/binary_inputformat_impl.hpp"

namespace husky {
namespace io {

class BinaryInputFormat final : public InputFormatBase {
   public:
    typedef BinaryInputFormatImpl::RecordT RecordT;
    typedef BinaryInputFormatImpl::CastRecordT CastRecordT;

    explicit BinaryInputFormat(const std::string& url, const std::string& filter = "");
    BinaryInputFormat(const BinaryInputFormat&) = delete;
    ~BinaryInputFormat();

    inline auto get_outchannels() { return infmt_impl_->get_outchannels(); }

    // The input of BinaryInputFormat is already fixed in the construtor
    inline bool is_setup() const { return true; }

    inline bool next(RecordT& t) { return infmt_impl_->next(t); }

    static inline CastRecordT& recast(RecordT& t) { return BinaryInputFormatImpl::recast(t); }

   private:
    BinaryInputFormatImpl* infmt_impl_ = nullptr;
};

}  // namespace io
}  // namespace husky
