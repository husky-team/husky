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
    auto& infmt1 = InputFormatStore::create_line_inputformat();
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_line_inputformat();
    EXPECT_EQ(InputFormatStore::size(), 2);
    InputFormatStore::drop_all_inputformats();
}

TEST_F(TestInputFormatStore, ChunkInputFormat) {
    auto& infmt1 = InputFormatStore::create_chunk_inputformat(1);
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_chunk_inputformat(1);
    EXPECT_EQ(InputFormatStore::size(), 2);
    InputFormatStore::drop_all_inputformats();
}

TEST_F(TestInputFormatStore, SeparatorInputFormat) {
    auto& infmt1 = InputFormatStore::create_separator_inputformat("");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_separator_inputformat("");
    EXPECT_EQ(InputFormatStore::size(), 2);
    InputFormatStore::drop_all_inputformats();
}

TEST_F(TestInputFormatStore, XMLInputFormat) {
    auto& infmt1 = InputFormatStore::create_xml_inputformat("", "");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_xml_inputformat("", "");
    EXPECT_EQ(InputFormatStore::size(), 2);
    InputFormatStore::drop_all_inputformats();
}

TEST_F(TestInputFormatStore, BinaryInputFormat) {
    auto& infmt1 = InputFormatStore::create_binary_inputformat("nfs:///dummy", "");
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_binary_inputformat("nfs:///dummy", "");
    EXPECT_EQ(InputFormatStore::size(), 2);
    InputFormatStore::drop_all_inputformats();
}

#ifdef WITH_THRIFT
// The namespace(etc. std::tuple) has collision between flume and gtest, which causes failure.
// The test case is leaved here but would not be compiled.
TEST_F(TestInputFormatStore, DISABLED_FlumeInputFormat) {
    auto& infmt1 = InputFormatStore::create_flume_inputformat("localhost", 2016);
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_flume_inputformat("localhost", 2016);
    EXPECT_EQ(InputFormatStore::size(), 2);
    InputFormatStore::drop_all_inputformats();
}
#endif

#ifdef WITH_MONGODB
TEST_F(TestInputFormatStore, MongoDBInputFormat) {
    auto& infmt1 = InputFormatStore::create_mongodb_inputformat();
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_mongodb_inputformat();
    EXPECT_EQ(InputFormatStore::size(), 2);
    InputFormatStore::drop_all_inputformats();
}
#endif

TEST_F(TestInputFormatStore, ElasticsearchInputFormat) {
    auto& infmt1 = InputFormatStore::create_elasticsearch_inputformat();
    EXPECT_EQ(InputFormatStore::size(), 1);
    auto& infmt2 = InputFormatStore::create_elasticsearch_inputformat();
    EXPECT_EQ(InputFormatStore::size(), 2);
    InputFormatStore::drop_all_inputformats();
}

}  // namespace
}  // namespace io
}  // namespace husky
