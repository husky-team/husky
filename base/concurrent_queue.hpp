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

#include <mutex>
#include <queue>
#include <utility>

namespace husky {
namespace base {

template <typename ElementT>
class ConcurrentQueue {
   public:
    ConcurrentQueue() {}

    // Has to use std::move for `&&` parameter.
    ConcurrentQueue(ConcurrentQueue&& queue) : queue_(std::move(queue.queue_)), size_(queue.size_) { queue.size_ = 0; }

    // Has to use std::move for `&&` parameter.
    ConcurrentQueue& operator=(ConcurrentQueue&& queue) {
        queue_ = std::move(queue.queue_);
        size_ = queue_.size_;
        queue.size_ = 0;
        return *this;
    }

    inline bool is_empty() const { return size_ == 0; }
    inline int size() const { return size_; }

    // Has to use std::move for `&&` parameter.
    void push(ElementT&& element) {
        operation_mutex_.lock();
        queue_.push(std::move(element));
        ++size_;
        operation_mutex_.unlock();
    }

    ElementT pop() {
        operation_mutex_.lock();
        ElementT element = std::move(queue_.front());
        queue_.pop();
        --size_;
        operation_mutex_.unlock();
        return element;
    }

   private:
    int size_ = 0;
    std::mutex operation_mutex_;
    std::queue<ElementT> queue_;
};

}  // namespace base
}  // namespace husky
