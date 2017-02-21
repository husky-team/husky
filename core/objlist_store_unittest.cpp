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
    ObjList<Obj>& objlist_0 = ObjListStore::create_objlist<Obj>();
    size_t objlist_0_id = objlist_0.get_id();
    EXPECT_EQ(ObjListStore::size(), 1);
    ObjList<Obj>& objlist_1 = ObjListStore::create_objlist<Obj>();
    size_t objlist_1_id = objlist_1.get_id();
    EXPECT_EQ(ObjListStore::size(), 2);
    ObjList<Obj>& objlist_2 = ObjListStore::create_objlist<Obj>();
    size_t objlist_2_id = objlist_2.get_id();
    EXPECT_EQ(ObjListStore::size(), 3);

    auto& objlist_3 = ObjListStore::get_objlist<Obj>(objlist_0_id);
    EXPECT_EQ(&objlist_0, &objlist_3);
    auto& objlist_4 = ObjListStore::get_objlist<Obj>(objlist_2_id);
    EXPECT_EQ(&objlist_2, &objlist_4);

    ObjListStore::drop_objlist(objlist_2_id);
    EXPECT_FALSE(ObjListStore::has_objlist(objlist_2_id));
    EXPECT_EQ(ObjListStore::size(), 2);
    ObjListStore::drop_objlist(objlist_0_id);
    EXPECT_FALSE(ObjListStore::has_objlist(objlist_0_id));
    EXPECT_EQ(ObjListStore::size(), 1);
    ObjListStore::drop_objlist(objlist_1_id);
    EXPECT_FALSE(ObjListStore::has_objlist(objlist_1_id));
    EXPECT_EQ(ObjListStore::size(), 0);
}

}  // namespace
}  // namespace husky
