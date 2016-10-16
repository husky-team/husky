#include "core/objlist.hpp"

#pragma once

namespace husky {

template <typename ObjT>
class ObjListForTest : public ::husky::ObjList<ObjT> {
   public:
    void test_deletion_finalize() {
        ObjList<ObjT>::deletion_finalize();
    }
    void test_sort() {
        ObjList<ObjT>::sort();
    }
    size_t test_get_sorted_size() const {
        return ObjList<ObjT>::get_sorted_size();
    }
    size_t test_get_num_del() const {
        return ObjList<ObjT>::get_num_del();
    }
    size_t test_get_hashed_size() const {
        return ObjList<ObjT>::get_hashed_size();
    }
};

}  // namespace husky
