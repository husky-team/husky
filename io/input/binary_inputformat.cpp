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

#include "io/input/binary_inputformat.hpp"

#include <string>

#include "core/utils.hpp"
#include "io/input/binary_inputformat_impl.hpp"
#ifdef WITH_HDFS
#include "io/input/hdfs_binary_inputformat.hpp"
#endif
#include "io/input/nfs_binary_inputformat.hpp"

namespace husky {
namespace io {

BinaryInputFormat::BinaryInputFormat(const std::string& url, const std::string& filter) {
    int first_colon = url.find("://");
    ASSERT_MSG(first_colon != std::string::npos, ("Cannot analyze protocol from " + url).c_str());
    std::string protocol = url.substr(0, first_colon);
    if (protocol == "nfs") {
        infmt_impl_ = new NFSBinaryInputFormat();
        infmt_impl_->set_input(url.substr(first_colon + 3), filter);
#ifdef WITH_HDFS
    } else if (protocol == "hdfs") {
        infmt_impl_ = new HDFSBinaryInputFormat();
        infmt_impl_->set_input(url.substr(first_colon + 3), filter);
#endif
    } else {
        ASSERT_MSG(false, ("Unknown protocol given to BinaryInputFormat: " + protocol).c_str());
    }
}

BinaryInputFormat::~BinaryInputFormat() {
    ASSERT_MSG(infmt_impl_ != nullptr, "Trying to destroy a illegal BinaryInputFormat object");
    delete infmt_impl_;
    infmt_impl_ = nullptr;
}

}  // namespace io
}  // namespace husky
