#include "io/input/inputformat_factory.hpp"

#include <string>

#include "gtest/gtest.h"

#include "base/session_local.hpp"

namespace husky {
namespace io {
namespace {

class TestInputFormatFactory : public testing::Test {
   public:
    TestInputFormatFactory() {}
    ~TestInputFormatFactory() {}

   protected:
    void SetUp() {}
    void TearDown() { husky::base::SessionLocal::get_thread_finalizers().clear(); }
};

TEST_F(TestInputFormatFactory, LineInputFormat) {
    // Create
    auto& infmt1 = InputFormatFactory::create_line_inputformat("infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    auto& infmt2 = InputFormatFactory::create_line_inputformat("infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 2);

    // Get
    auto& infmt3 = InputFormatFactory::get_line_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatFactory::get_line_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatFactory::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    InputFormatFactory::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 0);
}

TEST_F(TestInputFormatFactory, ChunkInputFormat) {
    // Create
    auto& infmt1 = InputFormatFactory::create_chunk_inputformat(1, "infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    auto& infmt2 = InputFormatFactory::create_chunk_inputformat(1, "infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 2);

    // Get
    auto& infmt3 = InputFormatFactory::get_chunk_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatFactory::get_chunk_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatFactory::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    InputFormatFactory::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 0);
}

TEST_F(TestInputFormatFactory, SeparatorInputFormat) {
    // Create
    auto& infmt1 = InputFormatFactory::create_separator_inputformat("", "infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    auto& infmt2 = InputFormatFactory::create_separator_inputformat("", "infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 2);

    // Get
    auto& infmt3 = InputFormatFactory::get_separator_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatFactory::get_separator_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatFactory::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    InputFormatFactory::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 0);
}

TEST_F(TestInputFormatFactory, XMLInputFormat) {
    // Create
    auto& infmt1 = InputFormatFactory::create_xml_inputformat("", "", "infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    auto& infmt2 = InputFormatFactory::create_xml_inputformat("", "", "infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 2);

    // Get
    auto& infmt3 = InputFormatFactory::get_xml_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatFactory::get_xml_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatFactory::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    InputFormatFactory::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 0);
}

TEST_F(TestInputFormatFactory, BinaryInputFormat) {
    // Create
    auto& infmt1 = InputFormatFactory::create_binary_inputformat("nfs:///dummy", "", "infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    auto& infmt2 = InputFormatFactory::create_binary_inputformat("nfs:///dummy", "", "infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 2);

    // Get
    auto& infmt3 = InputFormatFactory::get_binary_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatFactory::get_binary_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatFactory::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    InputFormatFactory::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 0);
}

#ifdef WITH_THRIFT
// Mark this case disabled since WITH_THRIFT is always set to false by CMakeLists.txt.
// The namespace(etc. std::tuple) has collision between flume and gtest, which causes this failure.
TEST_F(TestInputFormatFactory, DISABLED_FlumeInputFormat) {
    // Create
    auto& infmt1 = InputFormatFactory::create_flume_inputformat("localhost", 2016, "infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    auto& infmt2 = InputFormatFactory::create_flume_inputformat("localhost", 2016, "infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 2);

    // Get
    auto& infmt3 = InputFormatFactory::get_flume_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatFactory::get_flume_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatFactory::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    InputFormatFactory::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 0);
}
#endif

#ifdef WITH_MONGODB
TEST_F(TestInputFormatFactory, MongoDBInputFormat) {
    // Create
    auto& infmt1 = InputFormatFactory::create_mongodb_inputformat("infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    auto& infmt2 = InputFormatFactory::create_mongodb_inputformat("infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 2);

    // Get
    auto& infmt3 = InputFormatFactory::get_mongodb_inputformat("infmt1");
    EXPECT_EQ(&infmt1, &infmt3);
    auto& infmt4 = InputFormatFactory::get_mongodb_inputformat("infmt2");
    EXPECT_EQ(&infmt2, &infmt4);

    // Drop
    InputFormatFactory::drop_inputformat("infmt1");
    EXPECT_EQ(InputFormatFactory::size(), 1);
    InputFormatFactory::drop_inputformat("infmt2");
    EXPECT_EQ(InputFormatFactory::size(), 0);
}
#endif

}  // namespace
}  // namespace io
}  // namespace husky
