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

#ifdef WITH_ORC

#include <mutex>
#include <string>

#include "hdfs/hdfs.h"
#include "orc/OrcFile.hh"

#include "base/exception.hpp"
#include "base/log.hpp"
#include "base/thread_support.hpp"

namespace husky {
namespace io {

using husky::base::HuskyException;

const int kOrcRowBatchSize = 5000;

class HDFSFileInputStream final : public orc::InputStream {
   public:
    HDFSFileInputStream(hdfsFS hdfs_fs, const std::string& file) {
        hdfs_fs_ = hdfs_fs;
        file_name_ = file;
        hdfs_file_ = hdfsOpenFile(hdfs_fs_, file_name_.c_str(), O_RDONLY, 0, 0, 0);
        assert(hdfs_file_ != NULL);
        hdfsFileInfo* file_info = hdfsGetPathInfo(hdfs_fs_, file_name_.c_str());
        length_ = file_info->mSize;
        hdfsFreeFileInfo(file_info, 1);
    }

    ~HDFSFileInputStream() { hdfsCloseFile(hdfs_fs_, hdfs_file_); }

    uint64_t getLength() const override { return static_cast<uint64_t>(length_); }

    uint64_t getNaturalReadSize() const override { return 128 * 1024; }

    void read(void* buf, uint64_t length, uint64_t offset) override {
        if (!buf)
            throw HuskyException("Buffer is null");

        hdfsSeek(hdfs_fs_, hdfs_file_, offset);
        int32_t remain = static_cast<int32_t>(length);
        int32_t start = 0;
        int32_t nbytes = 0;
        while (remain > 0) {
            // only 128KB per hdfsRead
            nbytes = hdfsRead(hdfs_fs_, hdfs_file_, buf + start, remain);
            start += nbytes;
            remain -= nbytes;
        }

        if (start == -1)
            throw HuskyException("Bad read of " + file_name_);
        if (static_cast<uint64_t>(start) != length)
            throw HuskyException("Short read of " + file_name_);
    }

    const std::string& getName() const override { return file_name_; }

   private:
    std::string file_name_;
    hdfsFile hdfs_file_;
    hdfsFS hdfs_fs_;
    int64_t length_;
    std::mutex read_mutex;
};

std::unique_ptr<orc::InputStream> read_hdfs_file(hdfsFS hdfs_fs, const std::string& path) {
    return std::unique_ptr<orc::InputStream>(new HDFSFileInputStream(hdfs_fs, path));
}

}  // namespace io
}  // namespace husky

#endif
