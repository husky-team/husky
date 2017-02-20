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

#include "core/objlist_store.hpp"

#include <string>
#include <unordered_map>

#include "base/session_local.hpp"

namespace husky {

thread_local unsigned int ObjListStore::s_gen_objlist_id = 0;
thread_local ObjListMap* ObjListStore::s_objlist_map = nullptr;

// set finalize_all_objlists priority to Level1, the higher the level, the higher the priority
static thread_local base::RegSessionThreadFinalizer finalize_all_objlists(base::SessionLocalPriority::Level1, []() {
    ObjListStore::drop_all_objlists();
    ObjListStore::free_objlist_map();
});

bool ObjListStore::has_objlist(const std::string& id) {
    ObjListMap& objlist_map = get_objlist_map();
    return objlist_map.find(id) != objlist_map.end();
}

void ObjListStore::drop_objlist(const std::string& id) {
    ObjListMap& objlist_map = get_objlist_map();
    if (objlist_map.find(id) == objlist_map.end())
        throw base::HuskyException("ObjListStore::drop_objlist: ObjList id doesn't exist");

    delete objlist_map[id];
    objlist_map.erase(id);
}

void ObjListStore::drop_all_objlists() {
    if (s_objlist_map == nullptr)
        return;

    for (auto& objlist_pair : (*s_objlist_map))
        delete objlist_pair.second;

    s_objlist_map->clear();
}

void ObjListStore::init_objlist_map() {
    if (s_objlist_map == nullptr)
        s_objlist_map = new ObjListMap();
}

void ObjListStore::free_objlist_map() {
    delete s_objlist_map;
    s_objlist_map = nullptr;
}

ObjListMap& ObjListStore::get_objlist_map() {
    init_objlist_map();
    return *s_objlist_map;
}

}  // namespace husky
