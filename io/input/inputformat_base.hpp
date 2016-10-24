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

#include "boost/utility/string_ref.hpp"

#include "core/channel/channel_source.hpp"
#include "core/utils.hpp"

#ifdef WITH_HDFS
#include "io/input/hdfs_file_splitter.hpp"
#endif

#include "io/input/file_splitter_base.hpp"
#include "io/input/nfs_file_splitter.hpp"

namespace husky {
namespace io {

/// Interface for InputFormat.
class InputFormatBase : public husky::ChannelSource {
   public:
    virtual ~InputFormatBase() = default;
    /// Check if all configures are set up.
    virtual bool is_setup() const = 0;
    /// Base function for recasting record type
    template <typename RecordT>
    static RecordT& recast(RecordT& t) {
        return t;
    }
    /// function for creating different splitters for different urls
    void set_splitter(const std::string& url) {
        int prefix = url.find("://");
        ASSERT_MSG(prefix != std::string::npos, ("Cannot analyze protocol from " + url).c_str());
        std::string protocol = url.substr(0, prefix);
        if (protocol == "nfs") {
            if (splitter_ != nullptr && dynamic_cast<NFSFileSplitter*>(splitter_) == NULL) {
                delete splitter_;
                splitter_ = new NFSFileSplitter();
            } else if (splitter_ == nullptr) {
                splitter_ = new NFSFileSplitter();
            } else {}
        #ifdef WITH_HDFS
        } else if (protocol == "hdfs") {
            if (splitter_ != nullptr && dynamic_cast<HDFSFileSplitter*>(splitter_) == NULL) {
                delete splitter_;
                splitter_ = new HDFSFileSplitter();
            } else if (splitter_ == nullptr) {
                splitter_ =  new HDFSFileSplitter();
            } else {}
        #endif
        } else {
            ASSERT_MSG(false, ("Unknown protocol given to LineInputFormat: " + protocol).c_str());
        }
        splitter_->load(url.substr(prefix + 3)); 
    }

   protected:
    int is_setup_ = 0;
    FileSplitterBase* splitter_ = nullptr;
};

}  // namespace io
}  // namespace husky
