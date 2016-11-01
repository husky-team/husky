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

#include "core/accessor.hpp"

#include <string>
#include <unordered_set>

#include "core/utils.hpp"

namespace husky {

thread_local std::unordered_set<AccessorBase*> AccessorBase::access_states_;

AccessorBase::AccessorBase(const std::string class_name)
    : class_name_(class_name), commit_barrier_(new BarrierType()), access_barrier_(new GenerationLock()) {}

AccessorBase::AccessorBase(AccessorBase&& ab)
    : owner_can_access_collection_(ab.owner_can_access_collection_),
      class_name_(std::move(ab.class_name_)),
      num_unit_(ab.num_unit_),
      commit_barrier_(ab.commit_barrier_),
      access_barrier_(ab.access_barrier_) {
    ab.owner_can_access_collection_ = true;
    ab.num_unit_ = 0;
    ab.commit_barrier_ = new BarrierType();
    ab.access_barrier_ = new GenerationLock();
}

AccessorBase::~AccessorBase() {
    delete commit_barrier_;
    delete access_barrier_;
}

void AccessorBase::init(size_t num_unit) {
    if (num_unit_ == 0) {
        ASSERT_MSG(num_unit > 0, (class_name_ + ": number of unit should be larger than 0").data());
        num_unit_ = num_unit + 1;
        commit_barrier_->set_target_count(num_unit_);
    }
}

void AccessorBase::commit_to_visitors() {
    access_barrier_->notify();
    owner_can_access_collection_ = false;
}

void AccessorBase::require_init() {
    ASSERT_MSG(num_unit_ != 0, (class_name_ + ": owner needs to call `void init(num_unit)` to be initialized").data());
}

void AccessorBase::owner_barrier() {
    if (!owner_can_access_collection_) {
        commit_barrier_->lock(true);
        owner_can_access_collection_ = true;
    }
}

void AccessorBase::visitor_barrier() {
    if (!access_states_.count(this)) {
        access_barrier_->wait();
        access_states_.insert(this);
    }
}

}  // namespace husky
