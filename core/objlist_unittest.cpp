#include "core/objlist.hpp"

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
    ObjList<Obj>* obj_list = new ObjList<Obj>();
    ASSERT_TRUE(obj_list != nullptr);
    delete obj_list;
}

TEST_F(TestObjList, AddObject) {
    ObjList<Obj> obj_list;
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
    ObjList<Obj> obj_list;
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
    ObjList<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(10 - i - 1);
        obj_list.add_object(std::move(obj));
    }
    obj_list.sort();
    EXPECT_EQ(obj_list.get_sorted_size(), 10);
    EXPECT_EQ(obj_list.get_num_del(), 0);
    EXPECT_EQ(obj_list.get_hashed_size(), 0);
    EXPECT_EQ(obj_list.get_size(), 10);
    std::vector<Obj>& v = obj_list.get_data();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(v[i].key, i);
    }
}

TEST_F(TestObjList, Delete) {
    ObjList<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
    }
    std::vector<Obj>& v = obj_list.get_data();
    Obj* p = &v[3];
    Obj* p2 = &v[7];
    obj_list.delete_object(p);
    obj_list.delete_object(p2);
    EXPECT_EQ(obj_list.get_num_del(), 2);
    EXPECT_EQ(obj_list.get_del(3), 1);
    EXPECT_EQ(obj_list.get_del(5), 0);
    obj_list.deletion_finalize();
    EXPECT_EQ(obj_list.get_num_del(), 0);
    EXPECT_EQ(obj_list.get_size(), 8);
}

TEST_F(TestObjList, Find) {
    ObjList<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
    }
    EXPECT_NE(obj_list.find(3), nullptr);
    EXPECT_NE(obj_list.find(5), nullptr);
    EXPECT_EQ(obj_list.find(10), nullptr);
    obj_list.sort();
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

TEST_F(TestObjList, SetHashRing) {
    ObjList<Obj> obj_list;
    HashRing hash_ring;
    obj_list.set_hash_ring(hash_ring);
    hash_ring.insert(1, 0);
    obj_list.set_hash_ring(hash_ring);
}

}  // namespace
}  // namespace husky
