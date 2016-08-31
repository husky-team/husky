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

#include <string>
#include <vector>

#include "core/accessor.hpp"
#include "core/utils.hpp"

namespace husky {

/* *
* Two situations may occur:
* 1. Computation Bound
* A: ----------|---------------------computation(X)-------------------|-------------
*            commit                                                 commit
* B: |---------|>>>>>>>>>>>>>>|-------computation(X)-------|----------|>>>>>>>>>>>>>|
*  access    access'        leave                        access     access'       leave
*
* 2. Access Bound
* A: -------|----------|-------computation(X+1)------|---------------------|---------
*         commit    commit'                        commit               commit'
* B: |>>>>>>>>>>>>>>>>>|--------computation(X)----------|>>>>>>>>>>>>>>>>>>|---------
*  access            leave                            access             leave
*
* `generation` used here is to avoid a situation that some slow threads keep access
* too long, causing the owner thread unable to commit and so those fast threads will
* get out-of-date data if they start a new round of access.
*
* A. Every thread should commit before accessing data in other threads.
* B. After access, you should leave.
* C. Don't commit more than one time each round. I cannot stop you from this if you insist.
* */

template <typename CollectT>
class Shuffler : virtual public Accessor<CollectT> {
   public:
    typedef typename Accessor<CollectT>::NameType NameType;
    Shuffler(const NameType& name, unsigned max_visitor) : Accessor<CollectT>(name, max_visitor) {
        write_ = this->collection;
        this->collection = nullptr;
    }

    Shuffler(Shuffler<CollectT>&& s) : Accessor<CollectT>(std::move(s)), write_(s.write_) { s.write_ = nullptr; }

    virtual ~Shuffler() {
        // collection will be deleted in ~Accessor if ~Shuffler is invoked
        if (write_) {
            delete write_;
            write_ = nullptr;
        }
    }

    CollectT& storage() { return *write_; }

    void commit() {
        ++this->thread_generation.value();  // odd -> even
        this->wait_for_accessory();         // odd -> even
        if (this->collection)
            delete this->collection;
        this->collection = write_;
        this->num_visitor = this->max_visitor;
        this->to_be_accessible();           // even -> odd
        ++this->thread_generation.value();  // even -> odd
        write_ = new CollectT();
    }

   protected:
    CollectT* write_;
};

template <typename CellT>
class ShuffleCombiner {
   public:
    typedef std::vector<CellT> CollectT;
    typedef typename Shuffler<CollectT>::NameType NameType;

    ShuffleCombiner(const NameType& name, unsigned num_workers) {
        messages_.reserve(num_workers);
        for (unsigned i = 0; i < num_workers; ++i)
            messages_.push_back(Shuffler<CollectT>(name + std::to_string(i), 1));
    }

    ShuffleCombiner(ShuffleCombiner<CellT>&& s) : messages_(std::move(s.messages_)) {}

    virtual ~ShuffleCombiner() {}

    void init() {
        if (is_init_)
            return;
        is_init_ = true;

        for (auto& i : messages_)
            i.init();
    }

    void commit(unsigned idx) {
        assert(is_init_);
        messages_[idx].commit();
    }

    void leave(unsigned idx) {
        assert(is_init_);
        messages_[idx].leave();
    }

    CollectT& storage(unsigned idx) {
        assert(is_init_);
        return messages_[idx].storage();
    }

    CollectT& access(unsigned idx) { return messages_[idx].access(); }

   protected:
    std::vector<Shuffler<CollectT>> messages_;
    bool is_init_ = false;
};

}  // namespace husky
