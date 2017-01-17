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

/// 3 APIs are provided
/// create_objlist(), get_objlist(), drop_objlist()
/// See the unittest for the usages
class ObjListStore {
   public:
    template <typename ObjT>
    static ObjList<ObjT>& create_objlist(const std::string& name = "") {
        std::string list_name = name.empty() ? objlist_name_prefix + std::to_string(default_objlist_id++) : name;
        if(objlist_map.find(name) != objlist_map.end())
            throw base::HuskyException("ObjListStore::create_objlist: ObjList name already exists");
        auto* objlist = new ObjList<ObjT>();
        objlist_map.insert({list_name, objlist});
        return *objlist;
    }

    template <typename ObjT>
    static ObjList<ObjT>& get_objlist(const std::string& name) {
        if(objlist_map.find(name) == objlist_map.end())
            throw base::HuskyException("ObjListStore::get_objlist: ObjList name doesn't exist");
        auto* objlist = objlist_map[name];
        return *static_cast<ObjList<ObjT>*>(objlist);
    }

    static void drop_objlist(const std::string& name) {
        if(objlist_map.find(name) == objlist_map.end())
            throw base::HuskyException("ObjListStore::drop_objlist: ObjList name doesn't exist");
        delete objlist_map[name];
        objlist_map.erase(name);
    }

    static bool has_objlist(const std::string& name) { return objlist_map.find(name) != objlist_map.end(); }

    static size_t size() { return objlist_map.size(); }

    static void drop_all_objlists() {
        for (auto& objlist_pair : objlist_map) {
            delete objlist_pair.second;
        }
        objlist_map.clear();
    }

   protected:
    static thread_local std::unordered_map<std::string, ObjListBase*> objlist_map;
    static thread_local int default_objlist_id;
    static const char* objlist_name_prefix;
};

}  // namespace husky
