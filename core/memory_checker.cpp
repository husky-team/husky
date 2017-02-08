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

#include "core/memory_checker.hpp"

#include <functional>
#include <thread>

// TODO(legend): check if tcmalloc is included.
#include "gperftools/malloc_extension.h"

namespace husky {

const char* k_alloccated_bytes_str = "generic.current_allocated_bytes";
const char* k_heap_size_str = "generic.heap_size";

MemoryChecker::MemoryChecker(int seconds)
    : sleep_duration_(seconds), stop_thread_(false), checker_(), update_handler_(nullptr) {}

MemoryChecker::~MemoryChecker() {
    if (!stop_thread_)
        stop();
}

void MemoryChecker::serve() { checker_ = std::thread(&MemoryChecker::loop, this); }

void MemoryChecker::stop() {
    stop_thread_ = true;
    if (checker_.joinable())
        checker_.join();
}

void MemoryChecker::register_update_handler(const std::function<void()>& handler) { update_handler_ = handler; }

void MemoryChecker::loop() {
    while (!stop_thread_) {
        update();

        if (update_handler_ != nullptr)
            update_handler_();

        std::this_thread::sleep_for(sleep_duration_);
    }
}

void MemoryChecker::update() {
    MemoryInfo& info = get_memory_info();

    size_t allocated_bytes;
    MallocExtension::instance()->GetNumericProperty(k_alloccated_bytes_str, &allocated_bytes);
    size_t heap_size;
    MallocExtension::instance()->GetNumericProperty(k_heap_size_str, &heap_size);

    info.allocated_bytes = allocated_bytes;
    info.heap_size = heap_size;
}

}  // namespace husky
