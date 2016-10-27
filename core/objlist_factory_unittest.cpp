#include "core/objlist_factory.hpp"

#include "gtest/gtest.h"

#include "core/objlist.hpp"

namespace husky {
namespace {

class TestObjListFactory : public testing::Test {
   public:
    TestObjListFactory() {}
    ~TestObjListFactory() {}

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

TEST_F(TestObjListFactory, Create) {
    Context::init_global();
    // Normal Create
    ObjList<Obj> obj_list_normal1;

    // Create
    ObjList<Obj>& obj_list = ObjListFactory::create_objlist<Obj>();
    EXPECT_EQ(ObjListFactory::size(), 1);
    ObjList<Obj>& obj_list2 = ObjListFactory::create_objlist<Obj>();
    EXPECT_EQ(ObjListFactory::size(), 2);
    ObjList<Obj>& obj_list3 = ObjListFactory::create_objlist<Obj>("abc");
    EXPECT_EQ(ObjListFactory::size(), 3);

    // Get
    auto& obj_list4 = ObjListFactory::get_objlist<Obj>("abc");
    EXPECT_EQ(&obj_list3, &obj_list4);
    auto& obj_list5 = ObjListFactory::get_objlist<Obj>("default_objlist_1");
    EXPECT_EQ(&obj_list2, &obj_list5);

    // Drop
    ObjListFactory::drop_objlist("abc");
    EXPECT_EQ(ObjListFactory::size(), 2);
    ObjListFactory::drop_objlist("default_objlist_1");
    EXPECT_EQ(ObjListFactory::size(), 1);
    ObjListFactory::drop_objlist("default_objlist_0");
    EXPECT_EQ(ObjListFactory::size(), 0);
}

}  // namespace
}  // namespace husky
