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

#include <atomic>
#include <chrono>
#include <functional>
#include <thread>

namespace husky {

struct MemoryInfo {
    size_t allocated_bytes = 0;
    size_t heap_size = 0;
};

class MemoryChecker {
   public:
    explicit MemoryChecker(int seconds = 1);
    virtual ~MemoryChecker();

    void serve();
    void stop();
    void register_update_handler(const std::function<void()>& handler);

    static MemoryInfo& get_memory_info() {
        static MemoryInfo memory_info;
        return memory_info;
    }

   protected:
    void loop();
    void update();

   private:
    std::chrono::seconds sleep_duration_;
    std::atomic_bool stop_thread_;
    std::thread checker_;
    std::function<void()> update_handler_;
};

}  // namespace husky
