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
#include <mutex>
#include <string>

#include "base/log.hpp"

namespace husky {
namespace base {

class HBarrier {
   public:
    explicit HBarrier(int count);
    void wait();

   private:
    int count_;
    int total_;
    std::condition_variable cv_;
    std::mutex mutex_;
};

/// In case don't know the number of threads when create the barrier
class KBarrier {
   public:
    KBarrier();
    void wait(int num_threads);

   private:
    int count_;
    int generation_;
    std::condition_variable cv_;
    std::mutex mutex_;
};

extern int gCallOnceFlag;
extern thread_local int gCallOnceGeneration;
extern std::mutex gCallOnceMutex;

template <typename Func, typename... Args>
void call_once_each_time(const Func& func, Args&&... args) {
    gCallOnceGeneration++;

    std::lock_guard<std::mutex> guard(gCallOnceMutex);
    if (gCallOnceGeneration > gCallOnceFlag) {
        func(args...);
        gCallOnceFlag = gCallOnceGeneration;
    }
}

}  // namespace base
}  // namespace husky
