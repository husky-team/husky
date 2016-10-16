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

#include "gtest/gtest.h"

#include "core/objlist_unittest.hpp"

namespace husky {
namespace {

class TestAttrList : public testing::Test {
   public:
    TestAttrList() {}
    ~TestAttrList() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class Obj {
   public:
    using KeyT = int;
    KeyT key;
    const KeyT& id() const { return key; }
    Obj() = default;
    ~Obj() = default;
    explicit Obj(const KeyT k) : key(k) {}
};

class AttrDb {
   public:
    AttrDb() = default;
    ~AttrDb() = default;
    explicit AttrDb(const double v) : val(v) {}
    double val;
};

TEST_F(TestAttrList, InitAndGet) {
    ObjListForTest<Obj> obj_list;
    obj_list.create_attrlist<int>("int");
    obj_list.create_attrlist<AttrDb>("attr");
    auto& int_list = obj_list.get_attrlist<int>("int");
    auto& attr_list = obj_list.get_attrlist<AttrDb>("attr");
    auto idx = obj_list.add_object(Obj(1));
    int_list.set(idx, 10);
    EXPECT_EQ(int_list.size(), 1);
    EXPECT_EQ(attr_list.size(), 0);
}

TEST_F(TestAttrList, InitAndDelete) {
    ObjListForTest<Obj> obj_list;
    obj_list.create_attrlist<int>("int");
    EXPECT_EQ(obj_list.del_attrlist("int"), 1);
    EXPECT_EQ(obj_list.del_attrlist("int"), 0);
}

TEST_F(TestAttrList, SetAttrAndGet) {
    ObjListForTest<Obj> obj_list;
    auto& intlist = obj_list.create_attrlist<int>("int");
    auto& attr_list = obj_list.create_attrlist<AttrDb>("attr");
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        size_t idx = obj_list.add_object(obj);
        intlist.set(idx, i);
        attr_list.set(idx, AttrDb(static_cast<double>(i)));
    }
    obj_list.add_object(Obj(10));

    // Get an unset attribute
    size_t idx = obj_list.add_object(Obj(11));
    intlist.get(idx);

    // set & get by obj
    auto& objvec = obj_list.get_data();
    intlist.set(objvec[10], 123);
    attr_list.set(objvec[10], AttrDb(1.23));
    EXPECT_EQ(intlist.get(objvec[10]), 123);
    EXPECT_DOUBLE_EQ(attr_list.get(objvec[10]).val, 1.23);
    EXPECT_EQ(intlist[objvec[10]], 123);
    EXPECT_DOUBLE_EQ(attr_list[objvec[10]].val, 1.23);

    // get by index
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(intlist.get(i), i);
        EXPECT_DOUBLE_EQ(attr_list.get(i).val, static_cast<double>(i));
        // index by subscript
        EXPECT_EQ(intlist[i], i);
        EXPECT_DOUBLE_EQ(attr_list[i].val, static_cast<double>(i));
    }
}

TEST_F(TestAttrList, Sort) {
    ObjListForTest<Obj> obj_list;
    auto& intlist = obj_list.create_attrlist<int>("int");
    for (int i = 0; i < 10; ++i) {
        Obj obj(10 - i - 1);  // insert obj by descending order
        size_t idx = obj_list.add_object(std::move(obj));
        intlist.set(idx, i);  // attribute is in ascending order
    }
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(intlist[i], i);
    }

    obj_list.test_sort();

    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(intlist[i], 10 - i - 1);
    }
}

TEST_F(TestAttrList, DeleteAndSort) {
    ObjListForTest<Obj> obj_list;
    auto& intlist = obj_list.create_attrlist<int>("int");
    auto& attr_list = obj_list.create_attrlist<AttrDb>("attr");
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        size_t idx = obj_list.add_object(std::move(obj));
        intlist.set(idx, i);
        attr_list.set(idx, AttrDb(static_cast<double>(i)));
    }
    std::vector<Obj>& v = obj_list.get_data();
    Obj* p = &v[3];
    Obj* p2 = &v[7];
    obj_list.delete_object(p);
    obj_list.delete_object(p2);
    EXPECT_EQ(obj_list.test_get_num_del(), 2);
    EXPECT_EQ(obj_list.get_del(3), 1);
    EXPECT_EQ(obj_list.get_del(5), 0);

    obj_list.test_deletion_finalize();

    // check size
    EXPECT_EQ(intlist.size(), 8);
    EXPECT_EQ(attr_list.size(), 8);
    // check finalization results (before sort)
    EXPECT_EQ(intlist[3], 9);
    EXPECT_EQ(intlist[7], 8);
    EXPECT_DOUBLE_EQ(attr_list[3].val, 9.0);
    EXPECT_DOUBLE_EQ(attr_list[7].val, 8.0);

    obj_list.test_sort();

    EXPECT_EQ(v[3].key, 4);
    EXPECT_EQ(v[7].key, 9);
    EXPECT_EQ(intlist[3], 4);
    EXPECT_EQ(intlist[7], 9);
    EXPECT_DOUBLE_EQ(attr_list[3].val, 4.0);
    EXPECT_DOUBLE_EQ(attr_list[7].val, 9.0);
}

}  // namespace
}  // namespace husky
