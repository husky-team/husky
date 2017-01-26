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

    inline KeyT id() const { return key; }

    Obj() = default;
    explicit Obj(int key) : key(key) {}
};

void labeled_point_example() {
    auto& obj_list = husky::ObjListStore::create_objlist<Obj>();
    obj_list.add_object(Obj(husky::Context::get_global_tid()));
    husky::globalize(obj_list);

    auto& channel =
        husky::ChannelStore::create_push_channel<husky::lib::LabeledPoint<husky::lib::VectorXd, double>>(
            obj_list, obj_list);
    husky::lib::LabeledPoint<husky::lib::VectorXd, double> pt(
        husky::lib::VectorXd::Constant(5, 1, husky::Context::get_global_tid()), husky::Context::get_global_tid());

    channel.prepare_messages();
    channel.push(pt, 0);
    channel.flush();

    husky::list_execute(obj_list, [&channel](Obj& obj) {
        if (obj.key == 0) {
            husky::lib::VectorXd sum1(5, 1);
            double sum2 = 0;
            for (auto& pt : channel.get(obj)) {
                sum1 += pt.x;
                sum2 += pt.y;
            }
            // should show <number of thread> * (<number of thread> - 1) / 2.0, <number of thread> * (<number of thread>
            // - 1) / 2.0
            husky::LOG_I << sum1[0] << ", " << sum2;
        }
    });
}

int main(int argc, char** argv) {
    if (husky::init_with_args(argc, argv)) {
        husky::run_job(labeled_point_example);
        return 0;
    }
    return 1;
}
