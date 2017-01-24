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
#include <thread>


namespace husky {

class MemoryChecker {
   public:
    enum QueryType {
        /// gen -- generic, tcm -- tcmalloc
        gen_allocated,
        gen_heap_size,
        tcm_pageheap_free,
        tcm_pageheap_unmapped,
        tcm_slack,
        tcm_max_total_thread_cache,
        tcm_current_total_thread_cache,
    };

    MemoryChecker() : stop_thread_(false), checker_() {}

    ~MemoryChecker() {
        stop_thread_ = true;
        if (checker_.joinable())
            checker_.join();
    }

    /// this query function will return byte(s) value as the unit
    size_t get_tcmalloc_query(enum MemoryChecker::QueryType key);

    void serve();

   protected:
    void exec_memory_query_per_second();

   private:
    std::atomic_bool stop_thread_;
    std::thread checker_;
};

}  // namespace husky
