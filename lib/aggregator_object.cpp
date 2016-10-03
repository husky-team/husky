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

#include "lib/aggregator_object.hpp"

#include <bitset>
#include <cstdio>
#include <sstream>
#include <string>

namespace husky {
namespace lib {

const char kIsZero = 1 << 0;         // state
const char kIsUpdated = 1 << 1;      // state
const char kResetEachIter = 1 << 2;  // mark
const char kIsActive = 1 << 3;       // mark
const char kIsRemoved = 1 << 4;      // mark
const char kOptionMask = 0x1c;       // to indicate which bits are marks

AggregatorState::AggregatorState() {
    set_zero();
    set_non_updated();
    set_keep_aggregate();
    set_active();
    set_non_removed();
}

bool AggregatorState::is_zero() { return !!(state_ & kIsZero); }

void AggregatorState::set_zero() { state_ |= kIsZero; }

void AggregatorState::set_non_zero() { state_ &= ~kIsZero; }

bool AggregatorState::is_updated() { return !!(state_ & kIsUpdated); }

void AggregatorState::set_updated() { state_ |= kIsUpdated; }

void AggregatorState::set_non_updated() { state_ &= ~kIsUpdated; }

void AggregatorState::set_reset_each_iter() { state_ |= kResetEachIter; }

bool AggregatorState::need_reset_each_iter() { return !!(state_ & kResetEachIter); }

void AggregatorState::set_keep_aggregate() { state_ &= ~kResetEachIter; }

bool AggregatorState::is_active() { return !!(state_ & kIsActive); }

void AggregatorState::set_active() { state_ |= kIsActive; }

void AggregatorState::set_inactive() { state_ &= ~kIsActive; }

bool AggregatorState::is_removed() { return !!(state_ & kIsRemoved); }

void AggregatorState::set_removed() { state_ |= kIsRemoved; }

void AggregatorState::set_non_removed() { state_ &= ~kIsRemoved; }

void AggregatorState::sync_option(const AggregatorState& b) {
    // keep those state bits and copy mark bits from b
    state_ = (state_ & (~kOptionMask)) | (b.state_ & kOptionMask);
}

std::string AggregatorState::to_string() {
    std::ostringstream oss;
    oss << "Aggregator(" << this << "), state: " << std::bitset<8>(state_);
    return oss.str();
}

AggregatorState::~AggregatorState() {}

}  // namespace lib
}  // namespace husky
