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
#include "io/input/inputformat_base.hpp"

namespace husky {
namespace io {

// TODO(zzxx):
// 1. Avoid user from copying a BinStream from a FileBinStreamBase

class FileBinStreamBase : public BinStream {
   public:
    explicit FileBinStreamBase(const std::string& file);
    const std::string& get_filename();
    void* pop_front_bytes(size_t sz) override;

   protected:
    virtual void read(char*, size_t) = 0;

   private:
    std::string filename_;
    static const size_t DEFAULT_BUF_SIZE;
};

class BinaryInputFormatRecord {
   public:
    typedef BinStream CastRecordT;  // this defines what type of binstream is given to users

    void set_bin_stream(FileBinStreamBase* b);
    ~BinaryInputFormatRecord();
    CastRecordT& cast();

   private:
    void remove_if_not_null();
    FileBinStreamBase* bin_ = nullptr;
};

enum BinaryInputFormatImplSetUp { NotSetUp = 0, AllSetUp = 1 };

class BinaryInputFormatImpl : public InputFormatBase {
   public:
    typedef BinaryInputFormatRecord RecordT;
    typedef BinaryInputFormatRecord::CastRecordT CastRecordT;

    // Should not called by user
    bool is_setup() const override;
    virtual bool next(RecordT&) = 0;
    virtual void set_input(const std::string&, const std::string&) = 0;

    static CastRecordT& recast(RecordT& record) { return record.cast(); }

   protected:
    BinaryInputFormatImpl();
    void to_be_setup();
};

}  // namespace io
}  // namespace husky
