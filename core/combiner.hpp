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

#include <algorithm>
#include <set>
#include <utility>
#include <vector>

#include "boost/sort/spreadsort/spreadsort.hpp"

#include "base/log.hpp"

namespace husky {

struct HashCombinerBase {};

template <typename MsgT>
struct HashSumCombiner : public HashCombinerBase {
    static void combine(MsgT& val, MsgT const& inc) { val += inc; }
};

template <typename MsgT>
struct SumCombiner {
    static void combine(MsgT& val, MsgT const& inc) { val += inc; }
};

template <typename MsgT>
struct MinCombiner {
    static void combine(MsgT& val, MsgT const& other) {
        if (other < val)
            val = other;
    }
};

template <typename MsgT>
struct HashIdenCombiner : public HashCombinerBase {
    static void combine(MsgT& val, MsgT const& inc) {}
};

struct IdenCombiner {};

template <typename BufferT>
void adj_merge_same(BufferT& combine_buffer) {
    int l = 0;
    for (int r = 1; r < combine_buffer.size(); r++)
        if (combine_buffer[l] != combine_buffer[r]) {
            l += 1;
            if (l != r)
                combine_buffer[l] = combine_buffer[r];
        }

    if (combine_buffer.size() != 0)
        combine_buffer.resize(l + 1);
}

template <typename CombinerT, typename BufferT>
void adj_merge(BufferT& combine_buffer) {
    int l = 0;
    for (int r = 1; r < combine_buffer.size(); r++)
        if (combine_buffer[l].first == combine_buffer[r].first) {
            CombinerT::combine(combine_buffer[l].second, combine_buffer[r].second);
        } else {
            l += 1;
            if (l != r)
                combine_buffer[l] = combine_buffer[r];
        }

    if (combine_buffer.size() != 0)
        combine_buffer.resize(l + 1);
}

template <typename CombinerT, typename MsgT>
typename std::enable_if<std::is_same<IdenCombiner, CombinerT>::value>::type back_combine(std::vector<MsgT>& buffer,
                                                                                         const MsgT& msg) {
    if (buffer.size() == 0 || buffer.back() != msg)
        buffer.push_back(msg);
}

template <typename CombinerT, typename KeyT, typename MsgT>
typename std::enable_if<std::is_same<IdenCombiner, CombinerT>::value>::type back_combine(
    std::vector<std::pair<KeyT, MsgT>>& buffer, const KeyT& key, const MsgT& msg) {
    if (buffer.size() == 0 || buffer.back().first != key || buffer.back().second != msg)
        buffer.push_back(std::make_pair(key, msg));
}

template <typename CombinerT, typename MsgT>
typename std::enable_if<!std::is_same<IdenCombiner, CombinerT>::value>::type back_combine(std::vector<MsgT>& buffer,
                                                                                          const MsgT& msg) {
    if (buffer.size() == 0)
        buffer.push_back(msg);
    else
        CombinerT::combine(buffer.back(), msg);
}

template <typename CombinerT, typename KeyT, typename MsgT>
typename std::enable_if<!std::is_same<IdenCombiner, CombinerT>::value>::type back_combine(
    std::vector<std::pair<KeyT, MsgT>>& buffer, const KeyT& key, const MsgT& msg) {
    if ((!buffer.empty()) && buffer.back().first == key) {
        CombinerT::combine(buffer.back().second, msg);
    } else {
        buffer.push_back(std::make_pair(key, msg));
    }
}

template <typename MsgT, typename KeyT>
void sort_buffer_by_key(std::vector<std::pair<KeyT, MsgT>>& combine_buffer) {
    std::sort(combine_buffer.begin(), combine_buffer.end(),
              [](const std::pair<KeyT, MsgT>& a, const std::pair<KeyT, MsgT>& b) { return a.first < b.first; });
}

// For some reason MSVC does not deal with SFINAE very well
#ifndef _MSC_VER
void sort_buffer_by_key_msg(std::vector<std::pair<int, int>>& combine_buffer);
#endif

template <typename MsgT>
void sort_buffer_by_key(std::vector<std::pair<int, MsgT>>& combine_buffer) {
    auto bracket = [](const std::pair<int, MsgT>& x, size_t offset) {
        const int bit_shift = 8 * (sizeof(int) - offset - 1);
        uint8_t result = (x.first >> bit_shift) & 0xFF;
        if (offset == 0)
            result ^= 128;
        return result;
    };
    auto getsize = [](auto) { return sizeof(int); };
    auto lessthan = [](const std::pair<int, MsgT>& x, const std::pair<int, MsgT>& y) { return x.first < y.first; };
    boost::sort::spreadsort::string_sort(combine_buffer.begin(), combine_buffer.end(), bracket, getsize, lessthan);
}

void sort_buffer_by_key(std::vector<int>& combine_buffer);

template <typename CombinerT, typename KeyT, typename MsgT>
typename std::enable_if<!std::is_same<IdenCombiner, CombinerT>::value>::type combine_single(
    std::vector<std::pair<KeyT, MsgT>>& combine_buffer) {
    sort_buffer_by_key(combine_buffer);
    adj_merge<CombinerT>(combine_buffer);
}

template <typename CombinerT, typename KeyT, typename MsgT>
typename std::enable_if<std::is_same<IdenCombiner, CombinerT>::value>::type combine_single(
    std::vector<std::pair<KeyT, MsgT>>& combine_buffer) {
    sort_buffer_by_key_msg(combine_buffer);
    adj_merge_same(combine_buffer);
}

}  // namespace husky
