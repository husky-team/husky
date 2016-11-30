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

/**
 * State Machine of Accessor:
 *
 *                     destroy(O)
 *                          ^
 *                          |
 *         +------------ leave(V) <--------+
 *         |              | ^              |
 *         v              v |              |
 * +-> storage(O) ----> commit(O) ----> access(V) <-+
 * |    |  ^              ^                |        |
 * +----+  |              |                +--------+
 *         +------------ init(O)
 *
 * 0. Accessor needs to be initialized (by any thread) before being used;
 * 1. Visitors can access the collection only after owner commits;
 * 2. Owner can commit a new collection only after all visitors leave;
 * 3. Owner can update the collection only after all visitors leave.
 *
 * Note: Each thread must hold no more than one visitor unit for each Accessor.
 * Note: Accessor is not copyable, as it uses CounterBarrier
 *
 * */

#pragma once

#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "base/counter_barrier.hpp"
#include "base/generation_lock.hpp"

namespace husky {

using base::CounterBarrier;
using base::GenerationLock;

class AccessorBase {
   public:
    void init(size_t num_unit);

   protected:
    explicit AccessorBase(const std::string class_name);
    AccessorBase(AccessorBase&&);
    virtual ~AccessorBase();

    void commit_to_visitors();
    void owner_barrier();
    void visitor_barrier();
    void require_init();

    typedef base::CounterBarrier BarrierType;
    // typedef base::FutureCounterBarrier BarrierType;
    BarrierType* commit_barrier_ = nullptr;
    GenerationLock* access_barrier_ = nullptr;

    static thread_local std::unordered_set<AccessorBase*> access_states_;

   private:
    bool owner_can_access_collection_ = true;
    const std::string class_name_;
    size_t num_unit_ = 0;
};

/// Accessor is designed for collection accessing management.
/// A collection can be maintained inside Accessor and fetched using `storage()`,
/// or the collection can be passed into Accessor by `commit()`
/// Units can fetch the collection by `access()` once a unit has `commit()` this Accessor
/// Units should `leave()` after they don't use the collection any more.
/// A unit can commit this accessor again only when all the accessing units have left.
template <typename CollectT>
class Accessor : public AccessorBase {
   public:
    Accessor() : AccessorBase("Accessor") {}

    Accessor(Accessor&& a)
        : AccessorBase(std::move(a)), need_delete_collection_(a.need_delete_collection_), collection_(a.collection_) {
        a.collection_ = nullptr;
        need_delete_collection_ = a.need_delete_collection_;
    }

    virtual ~Accessor() {
        owner_barrier();
        delete_collection();
    }

    CollectT& storage() {
        require_init();
        init_collection_if_null();
        owner_barrier();
        return *collection_;
    }

    void commit(CollectT& collection) {
        require_init();
        owner_barrier();

        delete_collection();
        collection_ = &collection;

        commit_to_visitors();
    }

    void commit() {
        require_init();
        init_collection_if_null();
        owner_barrier();
        commit_to_visitors();
    }

    CollectT& access() {
        require_init();
        visitor_barrier();
        return *collection_;  // commit ensures collection is not nullptr;
    }

    void leave() {
        require_init();
        visitor_barrier();
        commit_barrier_->lock();
        access_states_.erase(this);
    }

   protected:
    explicit Accessor(const std::string& class_name) : AccessorBase(class_name) {}

    void delete_collection() {
        if (collection_) {
            if (need_delete_collection_) {
                delete collection_;
                need_delete_collection_ = false;
            }
            collection_ = nullptr;
        }
    }

    void init_collection_if_null() {
        if (collection_ == nullptr) {
            collection_ = new CollectT();
            need_delete_collection_ = true;
        }
    }

    bool need_delete_collection_ = false;
    CollectT* collection_ = nullptr;
};

}  // namespace husky
