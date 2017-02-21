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
#include <unordered_map>

#include "base/assert.hpp"
#include "base/exception.hpp"
#include "core/context.hpp"
#include "core/objlist.hpp"

namespace husky {

typedef std::unordered_map<size_t, ObjListBase*> ObjListMap;

class ObjListStore {
   public:
    template <typename ObjT>
    static ObjList<ObjT>& create_objlist() {
        ObjListMap& objlist_map = get_objlist_map();
        auto* objlist = new ObjList<ObjT>();
        objlist_map.insert({objlist->get_id(), objlist});
        return *objlist;
    }

    template <typename ObjT>
    static ObjList<ObjT>& get_objlist(size_t id) {
        ObjListMap& objlist_map = get_objlist_map();
        auto* objlist = objlist_map[id];
        return *static_cast<ObjList<ObjT>*>(objlist);
    }

    static bool has_objlist(size_t id);
    static void drop_objlist(size_t id);
    static void drop_all_objlists();

    static void init_objlist_map();
    static void free_objlist_map();
    static ObjListMap& get_objlist_map();

    static inline size_t size() {
        if (s_objlist_map == nullptr)
            return 0;
        return s_objlist_map->size();
    }

   protected:
    static thread_local ObjListMap* s_objlist_map;
};

}  // namespace husky
