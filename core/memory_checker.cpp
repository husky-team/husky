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

#include <thread>

#include "gperftools/malloc_extension.h"

#include "base/log.hpp"


namespace husky {

const char* k_property_name[7] = {
     "generic.current_allocated_bytes",
     "generic.heap_size",
     "tcmalloc.pageheap_free_bytes",
     "tcmalloc.pageheap_unmapped_bytes",
     "tcmalloc.slack_bytes",
     "tcmalloc.max_total_thread_cache_bytes",
     "tcmalloc.current_total_thread_cache_bytes",
 };

void MemoryChecker::exec_memory_query_per_second() {
    while (!stop_thread_) {
        size_t cur_bytes = get_tcmalloc_query(MemoryChecker::gen_allocated);
        LOG_I << cur_bytes << " Bytes";
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

size_t MemoryChecker::get_tcmalloc_query(enum MemoryChecker::QueryType key) {
    size_t cur_bytes;
    MallocExtension::instance()->GetNumericProperty(k_property_name[key], &cur_bytes);
    return cur_bytes;
}

void MemoryChecker::serve() {
    checker_ = std::thread(&MemoryChecker::exec_memory_query_per_second, this);
}

}  // namespace husky
