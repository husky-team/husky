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

#include <cassert>

#include "base/log.hpp"

namespace husky {

#define ASSERT_MSG(condition, message)   \
    do {                                 \
        bool cond = (condition);         \
        if (!cond) {                     \
            printf("%s\n", (message));   \
            assert(cond && (condition)); \
        }                                \
    } while (false)

#ifdef HUSKY_DEBUG
#define HUSKY_DEBUG_ON 1
#else
#define HUSKY_DEBUG_ON 0
#endif

#define debug_msg(msg)                     \
    do {                                   \
        if (HUSKY_DEBUG_ON)                \
            Base::log_msg(msg, LOG_DEBUG); \
    } while (0)

#define ID(cls, type)                      \
    typedef type KeyT;                     \
    KeyT key;                              \
    const KeyT& id() const { return key; } \
    cls() {}                               \
    cls(KeyT key) { this->key = key; }

}  // namespace husky
