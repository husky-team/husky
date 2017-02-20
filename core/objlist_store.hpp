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

#include "core/context.hpp"
#include "core/objlist.hpp"
#include "core/utils.hpp"

namespace husky {

typedef std::unordered_map<std::string, ObjListBase*> ObjListMap;

class ObjListStore {
   public:
    template <typename ObjT>
    static ObjList<ObjT>& create_objlist() {
        ObjListMap& objlist_map = get_objlist_map();
        std::string id = std::to_string(s_gen_objlist_id++);
        if (objlist_map.find(id) != objlist_map.end())
            throw base::HuskyException("ObjListStore::create_objlist: ObjList id already exists");

        // TODO(legend): will store id into a objlist like `new ObjList<ObjT>(id)`.
        auto* objlist = new ObjList<ObjT>();
        objlist_map.insert({id, objlist});
        return *objlist;
    }

    template <typename ObjT>
    static ObjList<ObjT>& get_objlist(const std::string& id) {
        ObjListMap& objlist_map = get_objlist_map();
        if (objlist_map.find(id) == objlist_map.end())
            throw base::HuskyException("ObjListStore::get_objlist: ObjList id doesn't exist");

        auto* objlist = objlist_map[id];
        return *static_cast<ObjList<ObjT>*>(objlist);
    }

    static bool has_objlist(const std::string& id);
    static void drop_objlist(const std::string& id);

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
    static thread_local unsigned int s_gen_objlist_id;
};

}  // namespace husky
