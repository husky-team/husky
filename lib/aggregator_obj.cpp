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

#include "lib/aggregator_obj.hpp"

#include <string>

namespace lib {

const char kKeepAggregate = 0;
const char kIsZero = 1;
const char kResetEachRound = 1 << 1;
const char kNeedUpdate = 1 << 2;

AggregatorState::AggregatorState() : flag_(0) {}

AggregatorState::~AggregatorState() {}

bool AggregatorState::is_non_zero() { return flag_ & kIsZero; }

bool AggregatorState::need_update() { return flag_ & kNeedUpdate; }

void AggregatorState::set_zero() {
    if (flag_ & kResetEachRound)
        flag_ &= ~kIsZero;
}

void AggregatorState::set_non_zero() { flag_ |= kIsZero; }

void AggregatorState::set_no_update() { flag_ &= ~kNeedUpdate; }

void AggregatorState::set_need_update() { flag_ |= kNeedUpdate; }

void AggregatorState::to_keep_aggregate() { flag_ &= ~kResetEachRound; }

void AggregatorState::to_reset_each_round() { flag_ |= kResetEachRound; }

std::string AggregatorState::to_string() {
    char str[30] = "Aggregator State @%p";
    // 20 refers to the maximum possible length that %p will be
    snprintf(str, sizeof(str), "Aggregator State @%p", this);
    return std::string(str);
}

}  // namespace lib
