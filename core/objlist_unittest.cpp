#include "core/objlist_unittest.hpp"

#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace husky {
namespace {

class TestObjList : public testing::Test {
   public:
    TestObjList() {}
    ~TestObjList() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

class Obj {
   public:
    using KeyT = int;
    KeyT key;
    const KeyT& id() const { return key; }
    Obj() {}
    explicit Obj(const KeyT& k) : key(k) {}
};

TEST_F(TestObjList, InitAndDelete) {
    ObjListForTest<Obj>* obj_list = new ObjListForTest<Obj>();
    ASSERT_TRUE(obj_list != nullptr);
    delete obj_list;
}

TEST_F(TestObjList, AddObject) {
    ObjListForTest<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(obj);
    }
    std::vector<Obj>& v = obj_list.get_data();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(v[i].key, i);
    }
}

TEST_F(TestObjList, AddMoveObject) {
    ObjListForTest<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
    }
    std::vector<Obj>& v = obj_list.get_data();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(v[i].key, i);
    }
}

TEST_F(TestObjList, Sort) {
    ObjListForTest<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(10 - i - 1);
        obj_list.add_object(std::move(obj));
    }
    obj_list.test_sort();
    EXPECT_EQ(obj_list.test_get_sorted_size(), 10);
    EXPECT_EQ(obj_list.test_get_num_del(), 0);
    EXPECT_EQ(obj_list.test_get_hashed_size(), 0);
    EXPECT_EQ(obj_list.get_size(), 10);
    std::vector<Obj>& v = obj_list.get_data();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(v[i].key, i);
    }
}

TEST_F(TestObjList, Delete) {
    ObjListForTest<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
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
    EXPECT_EQ(obj_list.test_get_num_del(), 0);
    EXPECT_EQ(obj_list.get_size(), 8);
}

TEST_F(TestObjList, Find) {
    ObjListForTest<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
    }
    EXPECT_NE(obj_list.find(3), nullptr);
    EXPECT_NE(obj_list.find(5), nullptr);
    EXPECT_EQ(obj_list.find(10), nullptr);
    obj_list.test_sort();
    EXPECT_NE(obj_list.find(3), nullptr);
    EXPECT_NE(obj_list.find(5), nullptr);
    EXPECT_EQ(obj_list.find(10), nullptr);
}

TEST_F(TestObjList, IndexOf) {
    ObjList<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
    }
    Obj* p = &obj_list.get_data()[2];
    Obj* p2 = &obj_list.get_data()[6];
    EXPECT_EQ(obj_list.index_of(p), 2);
    EXPECT_EQ(obj_list.index_of(p2), 6);
    Obj o;
    Obj* p3 = &o;
    try {
        obj_list.index_of(p3);
    } catch (...) {
    }
}

}  // namespace
}  // namespace husky
