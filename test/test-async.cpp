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

#include <string>
#include <vector>

#include "core/engine.hpp"

using namespace husky;

class Obj {
   public:
    using KeyT = int;

    Obj() = default;
    explicit Obj(const KeyT& k) : key(k) {}
    const KeyT& id() const { return key; }

    int key;
};

void test_async_push() {
    constexpr int num_obj = 30;
    auto& async_list = ObjListFactory::create_objlist<Obj>();
    auto& async_ch = ChannelFactory::create_async_push_channel<int>(async_list);
    if (Context::get_global_tid() == 0) {
        for (int i = 0; i < num_obj; ++i) {
            async_list.add_object(Obj(i));
        }
    }

    globalize(async_list);
    list_execute_async(
        async_list,
        [&async_ch](Obj& obj) {
            auto& msgs = async_ch.get(obj);
            if (obj.id() == 0) {
                if (msgs.size() > 0)
                    base::log_msg("msg content: " + std::to_string(msgs.at(0)) + ", " + std::to_string(msgs.at(1)));
                else
                    base::log_msg("No msg");
            }
            async_ch.push(obj.id(), (obj.id() + 1) % num_obj);
            async_ch.push(obj.id(), (obj.id() + 2) % num_obj);
        },
        4, 0.5);
}

void test_async_mig() {
    constexpr int num_obj = 30;
    auto& async_list = ObjListFactory::create_objlist<Obj>();
    auto& async_ch = ChannelFactory::create_async_migrate_channel<Obj>(async_list);
    if (Context::get_global_tid() == 0) {
        for (int i = 0; i < num_obj; ++i) {
            async_list.add_object(Obj(i));
        }
    }

    globalize(async_list);
    list_execute_async(
        async_list,
        [&async_ch](Obj& obj) {
            int tid = Context::get_global_tid();
            base::log_msg("obj_id: " + std::to_string(obj.id()) + " is in thread " + std::to_string(tid));
            async_ch.migrate(obj, 0);
        },
        3);
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    if (init_with_args(argc, argv, args)) {
        run_job(test_async_push);
        return 0;
    }
    return 1;
}
