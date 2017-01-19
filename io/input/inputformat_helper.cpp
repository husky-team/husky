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

#include "io/input/inputformat_helper.hpp"

#include <string>

#include "boost/utility/string_ref.hpp"

namespace husky {
namespace io {
namespace helper {

/// A helper function to find the next 'c' starting from index 'l'
size_t find_next(boost::string_ref sref, size_t l, char c) {
    size_t r = l;
    while (r != sref.size() && sref[r] != c)
        r++;
    if (r == sref.size())
        return boost::string_ref::npos;
    return r;
}

/// A helper function to find the last 'c' from the end
size_t find_last(boost::string_ref sref, char c) {
    size_t pos = sref.size() - 1;
    while (pos != 0 && sref[pos] != c)
        --pos;
    if (pos == 0 && sref[pos] != c)
        return boost::string_ref::npos;
    return pos;
}


/// A helper function to find the next 'str' starting from index 'l'
size_t find_next(boost::string_ref sref, size_t l, const std::string& str) {
    auto start = sref.substr(l);
    auto ret = start.find(str);
    if (ret == boost::string_ref::npos)
        return ret;
    return l + ret;
}

}  // namespace helper
}  // namespace io
}  // namespace husky
