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

#include <string>
#include <vector>

#include "base/log.hpp"
#include "base/serialization.hpp"
#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"

void read_bin_file(const std::string& path) {
    // path can be `nfs:///path/to/file` or `hdfs:///path/to/file`.
    auto& infmt = husky::io::InputFormatStore::create_binary_inputformat(path, "");

    size_t read_sz = 0;
    husky::load(infmt, [&read_sz](husky::base::BinStream& bin) { read_sz += bin.size(); });

    husky::LOG_I << std::to_string(husky::Context::get_global_tid()) << " reads " << std::to_string(read_sz)
                 << " bytes.";
}

int main(int argc, char** argv) {
    ASSERT_MSG(husky::init_with_args(argc, argv, {"path", "hdfs_namenode", "hdfs_namenode_port"}),
               "Argument `path` is not provided.");
    husky::run_job(std::bind(read_bin_file, husky::Context::get_param("path")));
}
