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
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace husky {

struct ThreadGenerationManager {
    // index
    std::list<unsigned> trash_bin;
    // <Name, <Count, Index> >
    std::unordered_map<std::string, std::pair<unsigned, unsigned>> name2id;
    std::mutex map_lock;
    unsigned cur_max_id = 0;

    void init() {
        trash_bin.clear();
        name2id.clear();
        cur_max_id = 0;
    }
};

class ThreadGeneration {
   public:
    typedef std::string NameType;
    NameType name;

    explicit ThreadGeneration(const NameType& name);
    ThreadGeneration(ThreadGeneration&& g);
    virtual ~ThreadGeneration();

    unsigned& value();
    void init();

    static ThreadGenerationManager& get_thread_generation_manager() { return TGManager; }

    static std::vector<unsigned>& get_shuffler_buffer() { return shuffler_buffer; }

   protected:
    unsigned index_;

    static unsigned& generate(const NameType& name);
    static void finalize(const NameType& name);

    static ThreadGenerationManager TGManager;
    static thread_local std::vector<unsigned> shuffler_buffer;
};

template <typename CollectT>
class Accessor {
    // Circle: commit -> access -> leave -> commit -> ...
   protected:
    bool has_init = false;
    ThreadGeneration thread_generation;
    CollectT* collection;
    unsigned generation, max_visitor;
    std::atomic_uint num_visitor;

    void to_be_accessible() { ++generation; }
    void wait_for_accessory() {
        while (thread_generation.value() > generation) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }

   public:
    typedef typename ThreadGeneration::NameType NameType;
    Accessor(const NameType& name, unsigned _max_visitor)
        : thread_generation(name),
          collection(new CollectT()),
          generation(2),  // even
          max_visitor(_max_visitor),
          num_visitor(0) {}
    Accessor(Accessor<CollectT>&& acc)
        : thread_generation(std::move(acc.thread_generation)),
          collection(acc.collection),
          generation(acc.generation),
          max_visitor(acc.max_visitor),
          num_visitor(acc.num_visitor.load()) {
        acc.collection = nullptr;
    }
    void init() {
        if (has_init)
            return;
        thread_generation.init();
        has_init = true;
    }
    CollectT& access() {
        wait_for_accessory();  // thread is odd, generation:even -> odd
        return *collection;
    }
    void leave() {
        wait_for_accessory();
        if (num_visitor.fetch_sub(1) == 1)
            to_be_accessible();  // odd -> even
    }
    CollectT& storage() {
        if (thread_generation.value() & 1) {  // is odd
            ++thread_generation.value();      // to be even
            wait_for_accessory();             // even to odd
        }                                     // is even
        return *collection;
    }
    void commit() {
        if (thread_generation.value() & 1) {  // is odd
            ++thread_generation.value();      // to be even
            wait_for_accessory();             // generation: odd -> even
        }                                     // thread is even
        num_visitor = max_visitor;
        to_be_accessible();           // generation:even -> odd
        ++thread_generation.value();  // odd
    }
    ~Accessor() {
        if (collection) {
            delete collection;
            collection = nullptr;
        }
        while (0 != num_visitor.load())
            std::this_thread::yield();
    }
};

}  // namespace husky
