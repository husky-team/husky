#include "core/objlist_store.hpp"

#include "gtest/gtest.h"

#include "base/session_local.hpp"
#include "core/objlist.hpp"

namespace husky {
namespace {

class TestObjListStore : public testing::Test {
   public:
    TestObjListStore() {}
    ~TestObjListStore() {}

   protected:
    void SetUp() {}
    void TearDown() { husky::base::SessionLocal::get_thread_finalizers().clear(); }
};

class Obj {
   public:
    using KeyT = int;
    KeyT key;
    const KeyT& id() const { return key; }
    Obj() {}
    explicit Obj(const KeyT& k) : key(k) {}
};

TEST_F(TestObjListStore, Functional) {
    ObjList<Obj>& obj_list_0 = ObjListStore::create_objlist<Obj>();
    EXPECT_EQ(ObjListStore::size(), 1);
    ObjList<Obj>& obj_list_1 = ObjListStore::create_objlist<Obj>();
    EXPECT_EQ(ObjListStore::size(), 2);
    ObjList<Obj>& obj_list_2 = ObjListStore::create_objlist<Obj>();
    EXPECT_EQ(ObjListStore::size(), 3);

    auto& obj_list_3 = ObjListStore::get_objlist<Obj>("0");
    EXPECT_EQ(&obj_list_0, &obj_list_3);
    auto& obj_list_4 = ObjListStore::get_objlist<Obj>("2");
    EXPECT_EQ(&obj_list_2, &obj_list_4);

    ObjListStore::drop_objlist("2");
    EXPECT_FALSE(ObjListStore::has_objlist("2"));
    EXPECT_EQ(ObjListStore::size(), 2);
    ObjListStore::drop_objlist("1");
    EXPECT_FALSE(ObjListStore::has_objlist("1"));
    EXPECT_EQ(ObjListStore::size(), 1);
    ObjListStore::drop_objlist("0");
    EXPECT_FALSE(ObjListStore::has_objlist("0"));
    EXPECT_EQ(ObjListStore::size(), 0);
}

}  // namespace
}  // namespace husky
