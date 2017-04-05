#include "core/objlist.hpp"

#include <algorithm>
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

    friend BinStream& operator<<(BinStream& stream, Obj& obj) { return stream << obj.key; }

    friend BinStream& operator>>(BinStream& stream, Obj& obj) { return stream >> obj.key; }
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

TEST_F(TestObjList, Shuffle) {
    std::vector<int> v;
    ObjList<Obj> obj_list;
    for (int i = 0; i < 10; ++i) {
        Obj obj(i);
        obj_list.add_object(std::move(obj));
        v.push_back(i);
    }
    obj_list.shuffle();
    EXPECT_EQ(obj_list.get_sorted_size(), 0);
    EXPECT_EQ(obj_list.get_num_del(), 0);
    EXPECT_EQ(obj_list.get_hashed_size(), 10);
    EXPECT_EQ(obj_list.get_size(), 10);
    int sum = 0;
    for (int i = 0; i < 10; ++i)
        sum += obj_list.get(i).key;
    EXPECT_EQ(sum, 45);
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

TEST_F(TestObjList, WriteAndRead) {
    ObjList<Obj> list_to_write;
    for (int i = 10; i > 0; --i) {
        Obj obj(i);
        list_to_write.add_object(std::move(obj));
    }
    list_to_write.sort();
    list_to_write.add_object(std::move(Obj(13)));
    list_to_write.add_object(std::move(Obj(12)));
    list_to_write.add_object(std::move(Obj(11)));
    std::vector<Obj>& v = list_to_write.get_data();
    Obj* p = &v[0];
    Obj* p2 = &v[10];
    list_to_write.delete_object(p);   // rm 1
    list_to_write.delete_object(p2);  // rm 13
    size_t old_objlist_size = list_to_write.get_size();

    EXPECT_TRUE(list_to_write.write_to_disk());
    size_t data_capacity_after_write = list_to_write.get_data().capacity();
    size_t bitmap_capacity_after_write = list_to_write.get_del_bitmap().capacity();
    EXPECT_EQ(data_capacity_after_write, 0);
    EXPECT_EQ(bitmap_capacity_after_write, 0);

    std::string list_to_write_path = list_to_write.id2str();
    ObjList<Obj> list_to_read;
    list_to_read.read_from_disk(list_to_write_path);

    EXPECT_EQ(list_to_read.get_size(), old_objlist_size);
    EXPECT_EQ(list_to_read.get_size(), list_to_read.get_sorted_size());
    EXPECT_EQ(list_to_read.get_size(), list_to_read.get_del_bitmap().size());
    EXPECT_EQ(list_to_read.get_hashed_size(), 0);
    EXPECT_EQ(list_to_read.get_num_del(), 0);

    for (size_t i = 0; i < list_to_read.get_size() - 1; i++)
        EXPECT_LE(list_to_read.get(i).id(), list_to_read.get(i + 1).id());
    for (size_t i = 0; i < list_to_read.get_size() - 1; i++)
        EXPECT_EQ(list_to_read.get_del(i), false);
}

TEST_F(TestObjList, EstimatedStorage) {
    const size_t len = 1000 * 1000 * 10;
    ObjList<Obj> test_list;
    for (size_t i = 0; i < len; ++i) {
        Obj obj(i);
        test_list.add_object(std::move(obj));
    }

    const double sample_rate = 0.005;
    size_t estimated_storage = test_list.estimated_storage_size(sample_rate);
    size_t real_storage = test_list.get_data().capacity() * sizeof(Obj);  // just in this class Obj case
    double diff_time =
        double(std::max(real_storage, estimated_storage)) / double(std::min(real_storage, estimated_storage));
    EXPECT_EQ(diff_time, 1);
}

}  // namespace
}  // namespace husky
