#include "base/session_local.hpp"

#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace husky {
namespace {

using base::SessionLocal;

class TestSessionLocal : public testing::Test {
   public:
    TestSessionLocal() {}
    ~TestSessionLocal() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestSessionLocal, Register) {
    SessionLocal::get_initializers().clear();
    SessionLocal::get_finalizers().clear();
    SessionLocal::get_thread_finalizers().clear();
    // Something that needs NOT to be initialized
    int session_no = 0;

    // Something that needs to be initialized
    std::string some_string = "";
    int some_int = -1;
    double some_double = -1.;
    struct SomeStruct {
        int64_t some_int64 = -1;
        std::pair<char, char> some_pair = {0, 0};
    } some_struct;
    std::vector<float> some_vec;

    EXPECT_EQ(SessionLocal::get_initializers().size(), 0);
    SessionLocal::register_initializer([&]() {
        some_string = "This is session " + std::to_string(session_no);
        some_int = session_no * session_no;
        some_double = 3.1415926535897932384626;
    });
    EXPECT_EQ(SessionLocal::get_initializers().size(), 1);
    SessionLocal::register_initializer([&]() {
        some_struct.some_int64 = (session_no + 0LL) << 40LL;
        some_struct.some_pair = std::make_pair('A', 'C');
    });
    EXPECT_EQ(SessionLocal::get_initializers().size(), 2);
    EXPECT_EQ(SessionLocal::get_finalizers().size(), 0);
    SessionLocal::register_finalizer([&]() { some_int = 0; });
    EXPECT_EQ(SessionLocal::get_finalizers().size(), 1);
    SessionLocal::register_initializer([&]() {
        some_vec.resize(10);
        for (int i = 0; i < some_vec.size(); i++) {
            some_vec[i] = 1.414f * i;
        }
    });
    SessionLocal::register_finalizer([&]() { some_vec.clear(); });
    EXPECT_EQ(SessionLocal::get_initializers().size(), 3);
    EXPECT_EQ(SessionLocal::get_finalizers().size(), 2);
    EXPECT_EQ(SessionLocal::is_session_end(), true);
    SessionLocal::get_initializers().clear();
    SessionLocal::get_finalizers().clear();
}

TEST_F(TestSessionLocal, MultiSessions) {
    SessionLocal::get_initializers().clear();
    SessionLocal::get_finalizers().clear();
    SessionLocal::get_thread_finalizers().clear();
    // Something that needs NOT to be initialized
    int session_no = 0;

    // Something that needs to be initialized
    std::string some_string = "";
    int some_int = -1;
    double some_double = -1.;
    struct SomeStruct {
        int64_t some_int64 = -1;
        std::pair<char, char> some_pair = {0, 0};
    } some_struct;
    std::vector<float> some_vec;

    SessionLocal::register_initializer([&]() {
        some_string = "This is session " + std::to_string(session_no);
        some_int = session_no * session_no;
        some_double = 3.1415926535897932384626;
    });
    SessionLocal::register_initializer([&]() {
        some_struct.some_int64 = (session_no + 0LL) << 40LL;
        some_struct.some_pair = std::make_pair('A', 'C');
    });
    SessionLocal::register_finalizer([&]() { some_int = 0; });
    SessionLocal::register_initializer([&]() {
        some_vec.resize(10);
        for (int i = 0; i < some_vec.size(); i++) {
            some_vec[i] = 1.414f * i;
        }
    });
    SessionLocal::register_finalizer([&]() { some_vec.clear(); });

    EXPECT_EQ(some_string.length(), 0);
    EXPECT_EQ(some_int, -1);
    EXPECT_EQ(some_double, -1.);
    EXPECT_EQ(some_struct.some_int64, -1LL);
    EXPECT_EQ(some_struct.some_pair.first, 0);
    EXPECT_EQ(some_struct.some_pair.second, 0);
    EXPECT_EQ(some_vec.size(), 0);

    for (session_no = 1; session_no < 10; session_no++) {
        SessionLocal::initialize();

        EXPECT_EQ(SessionLocal::is_session_end(), false);
        EXPECT_EQ(some_string, "This is session " + std::to_string(session_no));
        EXPECT_EQ(some_int, session_no * session_no);
        EXPECT_EQ(some_double, 3.1415926535897932384626);
        EXPECT_EQ(some_struct.some_int64, (session_no + 0LL) << 40LL);
        EXPECT_EQ(some_struct.some_pair.first, 'A');
        EXPECT_EQ(some_struct.some_pair.second, 'C');
        ASSERT_EQ(some_vec.size(), 10);
        for (int i = 0; i < some_vec.size(); i++) {
            EXPECT_EQ(some_vec[i], 1.414f * i);
        }

        SessionLocal::finalize();
        EXPECT_EQ(SessionLocal::is_session_end(), true);
        EXPECT_EQ(some_int, 0);
        EXPECT_EQ(some_vec.size(), 0);
    }

    SessionLocal::get_initializers().clear();
    SessionLocal::get_finalizers().clear();
}

TEST_F(TestSessionLocal, ThreadFinalizer) {
    SessionLocal::get_initializers().clear();
    SessionLocal::get_finalizers().clear();
    SessionLocal::get_thread_finalizers().clear();

    bool* some_bool = nullptr;
    int* some_int = nullptr;
    EXPECT_EQ(SessionLocal::get_thread_finalizers().empty(), true);
    // Level 1 is registered first so that it appears at the front of the vector
    SessionLocal::register_thread_finalizer(base::SessionLocalPriority::Level1, [&]() {
        EXPECT_NE(some_bool, nullptr);
        EXPECT_EQ(*some_bool, false);
        EXPECT_EQ(*some_int, 2);
        delete some_bool;
        delete some_int;
        some_bool = nullptr;
    });
    SessionLocal::register_thread_finalizer(base::SessionLocalPriority::Level2, [&]() {
        EXPECT_EQ(*some_int, 0);
        *some_int = 2;
    });
    EXPECT_EQ(SessionLocal::get_thread_finalizers().size(), 2);
    EXPECT_EQ(some_bool, nullptr);
    some_bool = new bool(false);
    some_int = new int(0);
    SessionLocal::thread_finalize();
    EXPECT_NE(some_bool, nullptr);  // expect only takes effect in a session

    SessionLocal::initialize();
    SessionLocal::thread_finalize();
    SessionLocal::finalize();
    EXPECT_EQ(some_bool, nullptr);

    SessionLocal::get_thread_finalizers().clear();
}

}  // namespace
}  // namespace husky
