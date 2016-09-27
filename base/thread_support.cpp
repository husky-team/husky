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

#include "base/thread_support.hpp"

#include <mutex>

#include "base/session_local.hpp"

namespace husky {
namespace base {

int gCallOnceFlag = 0;
thread_local int gCallOnceGeneration = 0;
std::mutex gCallOnceMutex;
RegSessionInitializer thread_support([]() { gCallOnceFlag = 0; });

HBarrier::HBarrier(int count) : count_(count), total_(count) {}

void HBarrier::wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    int cur_total = total_;
    if (--count_ == 0) {
        count_ = total_ < 0 ? -total_ : total_;
        total_ = -total_;
        cv_.notify_all();
        return;
    }
    while (cur_total == total_)
        cv_.wait(lock);
}

KBarrier::KBarrier() : count_(0), generation_(0) {}

void KBarrier::wait(int num_threads) {
    std::unique_lock<std::mutex> lock(mutex_);
    int cur_gen = generation_;
    if (++count_ == num_threads) {
        count_ = 0;
        generation_++;
        cv_.notify_all();
        return;
    }
    while (cur_gen == generation_)
        cv_.wait(lock);
}

}  // namespace base
}  // namespace husky
