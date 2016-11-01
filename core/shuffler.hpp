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

#include <vector>

#include "core/accessor.hpp"

namespace husky {

// TODO(zzxx): delete_collection can be invoked by the last one who leaves the shuffler
// Shuffler is designed for collection accessing management.
// A collection can be maintained inside a shuffler and fetched using `storage()`
// Units can access the collection when a unit has `commit()` this shuffler
// A unit can `commit()` this shuffler again only when all accessing units have `leave()`
// When other units are accessing the shuffler, owner unit can still use `storage()` to fetch the collection for write
template <typename CollectT>
class Shuffler : public Accessor<CollectT> {
   public:
    Shuffler() : Accessor<CollectT>("Shuffler") {}

    Shuffler(Shuffler&& s)
        : Accessor<CollectT>(std::move(s)), _write(s._write), _need_delete_write(s._need_delete_write) {
        s._write = nullptr;
        s._need_delete_write = false;
    }

    CollectT& storage() {
        this->require_init();
        init_write_if_null();
        return *_write;
    }

    void commit(CollectT& collection) {
        this->require_init();
        this->owner_barrier();
        _write = &collection;
        _need_delete_write = false;
        this->delete_collection();
        move_write_to_collection();
        this->commit_to_visitors();
    }

    void commit() {
        this->require_init();
        init_write_if_null();
        this->owner_barrier();
        this->delete_collection();
        move_write_to_collection();
        this->commit_to_visitors();
    }

    virtual ~Shuffler() {
        this->owner_barrier();
        delete_write();
    }

   protected:
    CollectT* _write = nullptr;
    bool _need_delete_write = false;

    void init_write_if_null() {
        if (_write == nullptr) {
            _write = new CollectT();
            _need_delete_write = true;
        }
    }

    void move_write_to_collection() {
        this->collection_ = _write;
        this->need_delete_collection_ = _need_delete_write;
        _write = nullptr;
        _need_delete_write = false;
    }

    void delete_write() {
        if (_write) {
            if (_need_delete_write) {
                delete _write;
                _need_delete_write = false;
            }
            _write = nullptr;
        }
    }
};

template <typename CellT>
class ShuffleCombiner {
   protected:
    typedef std::vector<CellT> CollectT;

   public:
    ShuffleCombiner() {}

    ShuffleCombiner(ShuffleCombiner&& sc) : messages_(std::move(sc.messages_)), num_units_(sc.num_units_) {
        sc.num_units_ = 0;
    }

    void init(int num_units) {
        num_units_ = num_units;
        messages_.resize(num_units_);
        for (Shuffler<CollectT>& s : messages_)
            s.init(1);
    }

    CollectT& storage(unsigned idx) { return messages_[idx].storage(); }

    void commit(unsigned idx) { messages_[idx].commit(); }

    CollectT& access(unsigned idx) { return messages_[idx].access(); }

    void leave(unsigned idx) { messages_[idx].leave(); }

   protected:
    std::vector<Shuffler<CollectT>> messages_;
    int num_units_ = 0;
};

}  // namespace husky
