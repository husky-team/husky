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

#include "base/generation_lock.hpp"

#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace husky {
namespace base {

thread_local std::unordered_map<GenerationBase*, std::pair<size_t, size_t>> GenerationBase::count_;

void GenerationBase::inc_generation() { ++generation_; }

size_t GenerationBase::inc_count() {
    std::pair<size_t, size_t>& self_gen = count_[this];
    if (self_gen.first != time_stamp_) {
        self_gen.first = time_stamp_;
        self_gen.second = 0;
    }
    return ++self_gen.second;
}

size_t GenerationBase::get_generation() { return generation_; }

void GenerationLock::notify() {
    inc_generation();
    std::unique_lock<std::mutex> _(notifier_mutex_);
    notifier_.notify_all();
}

void GenerationLock::wait() {
    size_t count = inc_count();
    if (count > get_generation()) {
        std::unique_lock<std::mutex> notifier_lock_(notifier_mutex_);
        while (count > get_generation()) {
            notifier_.wait(notifier_lock_);
        }
    }
}

void CallOnceEachTime::operator()(const std::function<void()>& exec) {
    size_t count = inc_count();
    std::lock_guard<std::mutex> _(mutex_);
    if (count > get_generation()) {
        exec();
        inc_generation();
    }
}

}  // namespace base
}  // namespace husky
