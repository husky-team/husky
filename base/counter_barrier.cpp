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

#include "base/counter_barrier.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

namespace husky {
namespace base {

void CounterBarrier::set_target_count(unsigned num) { target_ = num; }

void CounterBarrier::lock(bool should_wait) {
    status_mutex_.lock();
    if (++counter_ == target_) {
        counter_ = 0;
        hey_wake_up_ = true;
        status_mutex_.unlock();
        while (num_waiting_.load() != 0) {
            std::unique_lock<std::mutex> notifier_lock_(notifier_mutex_);
            notifier_.notify_all();
            std::this_thread::yield();
        }
        hey_wake_up_ = false;
    } else if (should_wait) {
        ++num_waiting_;
        status_mutex_.unlock();
        std::unique_lock<std::mutex> notifier_lock_(notifier_mutex_);
        while (!hey_wake_up_) {
            notifier_.wait(notifier_lock_);
        }
        --num_waiting_;
    } else {
        status_mutex_.unlock();
    }
}

FutureCounterBarrier::FutureCounterBarrier() { reset(); }

void FutureCounterBarrier::free_promise() {
    if (lock_) {
        delete lock_;
        lock_ = nullptr;
    }
}

void FutureCounterBarrier::reset() {
    counter_ = 0;
    free_promise();
    lock_ = new std::promise<bool>();
    future_ = lock_->get_future();
}

void FutureCounterBarrier::set_target_count(int num) { target_ = num; }

void FutureCounterBarrier::lock(bool should_wait) {
    {
        std::lock_guard<std::mutex> _(status_lock_);
        if (++counter_ == target_) {
            lock_->set_value(true);
            while (num_waiting_.load() != 0) {
                std::this_thread::yield();
            }
            reset();
            return;
        } else if (!should_wait) {
            return;
        }
        ++num_waiting_;
    }
    future_.wait();
    --num_waiting_;
}

FutureCounterBarrier::~FutureCounterBarrier() { free_promise(); }

}  // namespace base
}  // namespace husky
