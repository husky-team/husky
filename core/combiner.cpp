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

#include "core/combiner.hpp"

#include <utility>
#include <vector>

namespace husky {

void sort_buffer_by_key(std::vector<int>& combine_buffer) {
    boost::sort::spreadsort::spreadsort(combine_buffer.begin(), combine_buffer.end());
}

#ifdef _MSC_VER
void sort_buffer_by_key_msg(std::vector<std::pair<int, int>>& combine_buffer) {
    auto get_char = [](int x, size_t offset) {
        const int bit_shift = 8 * (sizeof(int) - offset - 1);
        uint8_t result = (x >> bit_shift) & 0xFF;
        if (offset == 0)
            result ^= 128;
        return result;
    };

    auto bracket = [&](const std::pair<int, int>& x, size_t offset) {
        if (offset < sizeof(int))
            return get_char(x.first, offset);
        return get_char(x.second, offset - sizeof(int));
    };
    auto getsize = [](auto) { return 2 * sizeof(int); };
    auto lessthan = [](const std::pair<int, int>& x, const std::pair<int, int>& y) { return x < y; };
    boost::sort::spreadsort::string_sort(combine_buffer.begin(), combine_buffer.end(), bracket, getsize, lessthan);
}
#endif

}  // namespace husky
