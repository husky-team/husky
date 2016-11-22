#include "core/objlist_store.hpp"

#include "gtest/gtest.h"

#include "core/objlist.hpp"

namespace husky {
namespace {

class TestObjListStore : public testing::Test {
   public:
    TestObjListStore() {}
    ~TestObjListStore() {}

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

TEST_F(TestObjListStore, Create) {
    Context::init_global();
    // Normal Create
    ObjList<Obj> obj_list_normal1;

    // Create
    ObjList<Obj>& obj_list = ObjListStore::create_objlist<Obj>();
    EXPECT_EQ(ObjListStore::size(), 1);
    ObjList<Obj>& obj_list2 = ObjListStore::create_objlist<Obj>();
    EXPECT_EQ(ObjListStore::size(), 2);
    ObjList<Obj>& obj_list3 = ObjListStore::create_objlist<Obj>("abc");
    EXPECT_EQ(ObjListStore::size(), 3);

    // Get
    auto& obj_list4 = ObjListStore::get_objlist<Obj>("abc");
    EXPECT_EQ(&obj_list3, &obj_list4);
    auto& obj_list5 = ObjListStore::get_objlist<Obj>("default_objlist_1");
    EXPECT_EQ(&obj_list2, &obj_list5);

    // Drop
    ObjListStore::drop_objlist("abc");
    EXPECT_EQ(ObjListStore::size(), 2);
    ObjListStore::drop_objlist("default_objlist_1");
    EXPECT_EQ(ObjListStore::size(), 1);
    ObjListStore::drop_objlist("default_objlist_0");
    EXPECT_EQ(ObjListStore::size(), 0);
}

}  // namespace
}  // namespace husky
