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

#include <string>

#include "boost/utility/string_ref.hpp"
#include "hdfs/hdfs.h"
#include "orc/OrcFile.hh"

#include "io/input/file_splitter_base.hpp"

namespace husky {
namespace io {

class ORCFileSplitter {
   public:
    ORCFileSplitter();
    virtual ~ORCFileSplitter();

    // intialize the url of the orc file
    virtual void load(std::string url);
    boost::string_ref fetch_batch();

   protected:
    boost::string_ref read_by_batch(size_t offset);
    std::string buffer_;
    // url may be a directory or a file
    std::string url_;
    // current filename
    std::string cur_fn_;
    // orc reader to help to read orc files
    std::unique_ptr<orc::Reader> reader_;

    hdfsFS fs_;
};

}  // namespace io
}  // namespace husky

#endif
