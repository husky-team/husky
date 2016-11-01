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

#include "core/engine.hpp"
#include "lib/vector.hpp"

class Obj {
public:
    typedef int KeyT;
    KeyT key;

    inline KeyT id() const {
        return key;
    }

    Obj() = default;
    explicit Obj(int key) : key(key) {}
};

void vector_example() {
    auto& obj_list = husky::ObjListFactory::create_objlist<Obj>();
    obj_list.add_object(Obj(husky::Context::get_global_tid()));
    husky::globalize(obj_list);

    auto& channel = husky::ChannelFactory::create_push_combined_channel<
            husky::lib::DenseVector<double>, husky::SumCombiner<husky::lib::DenseVector<double>>>(obj_list, obj_list);
    husky::lib::DenseVector<double> vec1(5, husky::Context::get_global_tid());

    channel.prepare_messages();
    channel.push(vec1, 0);
    channel.flush();

    husky::list_execute(obj_list, [&channel](Obj& obj) {
        if (obj.key == 0) {
            // should show <number of thread> * (<number of thread> - 1) / 2.0
            husky::base::log_msg(std::to_string(channel.get(obj)[0]));
        }
    });
}

int main(int argc, char** argv) {
    if (husky::init_with_args(argc, argv)) {
        husky::run_job(vector_example);
        return 0;
    }
    return 1;
}

