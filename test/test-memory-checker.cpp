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

#include <functional>

#include "base/log.hpp"
#include "core/memory_checker.hpp"

int main(int argc, char** argv) {
    husky::MemoryChecker memory_checker;
    memory_checker.serve();
    memory_checker.register_update_handler([]() {
        husky::MemoryInfo info = husky::MemoryChecker::get_memory_info();
        husky::LOG_I << "Allocated bytes: " << info.allocated_bytes;
        husky::LOG_I << "Heap size: " << info.heap_size;
    });
    std::this_thread::sleep_for(std::chrono::seconds(3));
    void* a = malloc(1000000000);
    std::this_thread::sleep_for(std::chrono::seconds(4));
    free(a);
    std::this_thread::sleep_for(std::chrono::seconds(3));
    return 0;
}
