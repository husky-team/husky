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

#include <array>
#include <cassert>

namespace husky {
namespace base {

template <typename ValT>
class ConcurrentChannelStore {
   public:
    static const int kNCol = 50;
    static const int kNRow = 50;

    ValT& get(int x, int y) {
        assert(x < 50);
        return cells_[x][y % 50];
    }

    void init(ValT val) {
        for (auto& row : cells_)
            for (auto& col : row)
                col = val;
    }

   protected:
    std::array<std::array<ValT, kNCol>, kNRow> cells_;
};

}  // namespace base
}  // namespace husky
