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

#include "base/disk_store.hpp"

#include <algorithm>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "base/serialization.hpp"

namespace husky {
namespace base {

DiskStore::DiskStore(const std::string& path) : path_(path) {}

// TODO(legend): Will use `pread` or other asio functions instead.
BinStream DiskStore::read() {
    std::vector<char> buffer;
    std::ifstream file;
    file.open(path_, std::ifstream::in | std::ifstream::binary);
    std::copy(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>(), std::back_inserter(buffer));
    file.close();

    return BinStream(std::move(buffer));
}

// TODO(legend): Will use `pwrite` or other asio functions instead.
bool DiskStore::write(BinStream&& bs) {
    if (bs.get_buffer_vector().empty())
        return false;

    std::ofstream file;
    file.open(path_, std::ofstream::out | std::ofstream::binary);
    if (!file)
        return false;

    std::copy(bs.get_buffer_vector().begin(), bs.get_buffer_vector().end(), std::ostreambuf_iterator<char>(file));
    file.close();
    return true;
}

}  // namespace base
}  // namespace husky
