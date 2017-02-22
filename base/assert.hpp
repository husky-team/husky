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

#include "base/log.hpp"

namespace husky {

bool assert_check(bool condition);

/// Usage:
///     husky::ASSERT(a == b);
///     husky::ASSERT_MSG(a == b, "a != b so fail");
///     husky::DASSERT(a == b);  // for debug mode
///     husky::DASSERT_MSG(a == b, "a != b so fail");  // for debug mode

#define CHECK(condition) assert_check(!(condition)) ? (void) 0 : husky::base::HuskyLoggerVoidify() & husky::LOG_F

#define DCHECK(condition) assert_check(!(condition)) ? (void) 0 : husky::base::HuskyLoggerVoidify() & husky::DLOG_F

#define ASSERT(condition) CHECK(!(condition)) << "Assert failed: " #condition

#define ASSERT_MSG(condition, message) CHECK(!(condition)) << "Assert failed: " #condition "\n" #message

#define DASSERT(condition) DCHECK(!(condition)) << "Assert failed: " #condition

#define DASSERT_MSG(condition, message) DCHECK(!(condition)) << "Assert failed: " #condition "\n" #message

}  // namespace husky
