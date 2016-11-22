#include "io/input/inputformat_store.hpp"

#include <string>

#include "gtest/gtest.h"

#include "base/session_local.hpp"

namespace husky {
namespace io {
namespace {

class TestInputFormatStore : public testing::Test {
   public:
    TestInputFormatStore() {}
    ~TestInputFormatStore() {}

   protected:
    void SetUp() {}
    void TearDown() { husky::base::SessionLocal::get_thread_finalizers().clear(); }
};

TEST_F(TestInputFormatStore, LineInputFormat) {
    // Create
    auto& infmt1 = InputFormatStore::create_line_inputformat("infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_line_inputformat("infmt2");
    EXPECT_EQ(InputFormatStore::size(), 2);

    // Get
    auto& infmt3 = InputFormatStore::get_line_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatStore::get_line_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatStore::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    InputFormatStore::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatStore::size(), 0);
}

TEST_F(TestInputFormatStore, ChunkInputFormat) {
    // Create
    auto& infmt1 = InputFormatStore::create_chunk_inputformat(1, "infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_chunk_inputformat(1, "infmt2");
    EXPECT_EQ(InputFormatStore::size(), 2);

    // Get
    auto& infmt3 = InputFormatStore::get_chunk_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatStore::get_chunk_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatStore::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    InputFormatStore::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatStore::size(), 0);
}

TEST_F(TestInputFormatStore, SeparatorInputFormat) {
    // Create
    auto& infmt1 = InputFormatStore::create_separator_inputformat("", "infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_separator_inputformat("", "infmt2");
    EXPECT_EQ(InputFormatStore::size(), 2);

    // Get
    auto& infmt3 = InputFormatStore::get_separator_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatStore::get_separator_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatStore::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    InputFormatStore::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatStore::size(), 0);
}

TEST_F(TestInputFormatStore, XMLInputFormat) {
    // Create
    auto& infmt1 = InputFormatStore::create_xml_inputformat("", "", "infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_xml_inputformat("", "", "infmt2");
    EXPECT_EQ(InputFormatStore::size(), 2);

    // Get
    auto& infmt3 = InputFormatStore::get_xml_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatStore::get_xml_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatStore::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    InputFormatStore::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatStore::size(), 0);
}

TEST_F(TestInputFormatStore, BinaryInputFormat) {
    // Create
    auto& infmt1 = InputFormatStore::create_binary_inputformat("nfs:///dummy", "", "infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_binary_inputformat("nfs:///dummy", "", "infmt2");
    EXPECT_EQ(InputFormatStore::size(), 2);

    // Get
    auto& infmt3 = InputFormatStore::get_binary_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatStore::get_binary_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatStore::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    InputFormatStore::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatStore::size(), 0);
}

#ifdef WITH_THRIFT
// Mark this case disabled since WITH_THRIFT is always set to false by CMakeLists.txt.
// The namespace(etc. std::tuple) has collision between flume and gtest, which causes this failure.
TEST_F(TestInputFormatStore, DISABLED_FlumeInputFormat) {
    // Create
    auto& infmt1 = InputFormatStore::create_flume_inputformat("localhost", 2016, "infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_flume_inputformat("localhost", 2016, "infmt2");
    EXPECT_EQ(InputFormatStore::size(), 2);

    // Get
    auto& infmt3 = InputFormatStore::get_flume_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatStore::get_flume_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatStore::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    InputFormatStore::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatStore::size(), 0);
}
#endif

#ifdef WITH_MONGODB
TEST_F(TestInputFormatStore, MongoDBInputFormat) {
    // Create
    auto& infmt1 = InputFormatStore::create_mongodb_inputformat("infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_mongodb_inputformat("infmt2");
    EXPECT_EQ(InputFormatStore::size(), 2);

    // Get
    auto& infmt3 = InputFormatStore::get_mongodb_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatStore::get_mongodb_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatStore::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatStore::size(), 1);
    InputFormatStore::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatStore::size(), 0);
}
#endif

}  // namespace
}  // namespace io
}  // namespace husky
