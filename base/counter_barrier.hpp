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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>

namespace husky {
namespace base {

// CounterBarrier is designed for thread synchronization.
// By calling `lock()`, units can increase the counter of the barrier.
// And units can choose whether to wait or not when calling `lock()`
// If choose to wait, units will keep waiting until the counter reaches target count.
// Target count is set by calling `set_target_count`
// FutureCounterBarrier is just a second implementation of CounterBarrier.

class CounterBarrier {
   public:
    void set_target_count(unsigned num);
    void lock(bool should_wait = false);

   private:
    unsigned counter_ = 0;
    unsigned target_ = 0;
    std::atomic_uint num_waiting_{0};
    bool hey_wake_up_ = false;
    unsigned status_idx_ = 0;
    std::mutex status_mutex_, notifier_mutex_;
    std::condition_variable notifier_;
};

class FutureCounterBarrier {
   public:
    FutureCounterBarrier();
    ~FutureCounterBarrier();
    void set_target_count(int num);
    void lock(bool should_wait = false);

   private:
    void free_promise();
    void reset();

    int counter_ = 0;
    int target_ = 0;
    std::atomic_uint num_waiting_{0};
    std::mutex status_lock_;
    std::promise<bool>* lock_{nullptr};
    std::future<bool> future_;
};

}  // namespace base
}  // namespace husky
