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

#include <condition_variable>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "base/time.hpp"

namespace husky {
namespace base {

// Base class for generation concepts
// Generation of the object and self-generation of each thread on this object is maintained here.
class GenerationBase {
   protected:
    void inc_generation();
    size_t inc_count();
    size_t get_generation();

   private:
    size_t time_stamp_ = base::get_current_time_milliseconds();
    size_t generation_ = 0;
    static thread_local std::unordered_map<GenerationBase*, std::pair<size_t, size_t>> count_;
};

// GenerationLock is designed for thread synchronization.
// By calling `wait()`, the unit will increase its generation and check if its generation is ahead of the global one.
// If so, the unit will keep waiting here, otherwise the unit will pass away.
// By calling `notify()`, the global generation is increased and waiting units will be notified.
class GenerationLock : public GenerationBase {
   public:
    // increase global generation, notify
    void notify();
    // increate self generation, wait
    void wait();

   private:
    std::condition_variable notifier_;
    std::mutex notifier_mutex_;
};

// CallOnceEachTime is designed for that each time when a group of threads need to execute
// a lambda, this lambda will be executed only once by the first thread.
class CallOnceEachTime : public GenerationBase {
   public:
    void operator()(const std::function<void()>& exec);

   private:
    std::mutex mutex_;
};

}  // namespace base
}  // namespace husky
