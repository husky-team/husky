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

#include "core/accessor.hpp"

#include <string>
#include <vector>

#include "core/utils.hpp"

namespace husky {

ThreadGenerationManager ThreadGeneration::TGManager;
thread_local std::vector<unsigned> ThreadGeneration::shuffler_buffer;

ThreadGeneration::ThreadGeneration(const ThreadGeneration::NameType& name) : name(name), index_(generate(name)) {}

ThreadGeneration::ThreadGeneration(ThreadGeneration&& g) : name(g.name), index_(g.index_) {
    g.name = ThreadGeneration::NameType();
}

ThreadGeneration::~ThreadGeneration() {
    if (name != NameType())
        finalize(name);
}

void ThreadGeneration::init() {
    auto& buffer = get_shuffler_buffer();
    if (buffer.size() <= index_) {
        buffer.resize(index_ + 1, 1u);
    }
    buffer[index_] = 1;
}

unsigned& ThreadGeneration::value() {
    ASSERT_MSG(get_shuffler_buffer().size() > index_,
               ("Expected: larger than " + std::to_string(get_shuffler_buffer().size()) + ", in fact: " +
                std::to_string(index_))
                   .c_str());
    return get_shuffler_buffer()[index_];
}

unsigned& ThreadGeneration::generate(const ThreadGeneration::NameType& name) {
    auto& manager = get_thread_generation_manager();
    std::lock_guard<std::mutex> guard(manager.map_lock);
    auto& id = manager.name2id[name];
    if (++id.first > 1) {
        return id.second;
    }
    if (manager.trash_bin.empty()) {
        id.second = manager.cur_max_id++;
    } else {
        id.second = manager.trash_bin.back();
        manager.trash_bin.pop_back();
    }
    return id.second;
}

void ThreadGeneration::finalize(const ThreadGeneration::NameType& name) {
    try {
        auto& manager = get_thread_generation_manager();
        std::lock_guard<std::mutex> guard(manager.map_lock);
        const auto& iter = manager.name2id.find(name);
        assert(manager.name2id.size() != 0);
        assert(iter != manager.name2id.end());

        if (--iter->second.first == 0) {
            manager.trash_bin.push_front(iter->second.second);
            manager.name2id.erase(iter);
        }
    } catch (std::string error_string) {
        if (error_string != "Context destroyed")
            throw;
    }
}

}  // namespace husky
