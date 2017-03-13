// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifdef WITH_ORC

#include "io/input/orc_file_splitter.hpp"

#include <string>
#include <vector>

#include "boost/utility/string_ref.hpp"
#ifdef WITH_HDFS
#include "hdfs/hdfs.h"
#endif
#include "orc/ColumnPrinter.hh"
#include "orc/OrcFile.hh"
#include "orc/orc-config.hh"

#include "base/log.hpp"
#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/coordinator.hpp"
#include "io/input/orc_hdfs_inputstream.hpp"

namespace orc {

class SQLColumnPrinter : public ColumnPrinter {
   public:
    SQLColumnPrinter(std::string& buffer, const Type& type) : ColumnPrinter(buffer) {
        for (unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
            // fieldNames.push_back(type.getFieldName(i));
            fieldPrinter.push_back(createColumnPrinter(buffer, type.getSubtype(i)).release());
        }
    }

    virtual ~SQLColumnPrinter() {
        for (size_t i = 0; i < fieldPrinter.size(); ++i) {
            delete fieldPrinter[i];
        }
    }

    void printRow(uint64_t rowId) override {
        if (hasNulls && !notNull[rowId]) {
            writeString(buffer, "null");
        } else {
            // writeChar(buffer, '{');
            for (unsigned int i = 0; i < fieldPrinter.size(); ++i) {
                if (i != 0) {
                    writeString(buffer, "\t");
                }
                // writeChar(buffer, '"');
                // writeString(buffer, fieldNames[i].c_str());
                // writeString(buffer, "\": ");
                fieldPrinter[i]->printRow(rowId);
            }
            // writeChar(buffer, '}');
        }
    }
    void reset(const ColumnVectorBatch& batch) override {
        ColumnPrinter::reset(batch);
        const StructVectorBatch& structBatch = dynamic_cast<const StructVectorBatch&>(batch);
        for (size_t i = 0; i < fieldPrinter.size(); ++i) {
            fieldPrinter[i]->reset(*(structBatch.fields[i]));
        }
    }

   private:
    void writeChar(std::string& file, char ch) { file += ch; }

    void writeString(std::string& file, const char* ptr) {
        size_t len = strlen(ptr);
        file.append(ptr, len);
    }

    std::vector<ColumnPrinter*> fieldPrinter;
};

}  // namespace orc

namespace husky {
namespace io {

using orc::ColumnVectorBatch;
using orc::createReader;
using orc::ReaderOptions;
using orc::readLocalFile;
using orc::SQLColumnPrinter;

// default number of lines in one read operation
// size_t ORCFileSplitter::row_batch_size = 8 * 1024;

ORCFileSplitter::ORCFileSplitter() {}

ORCFileSplitter::~ORCFileSplitter() { hdfsDisconnect(fs_); }
// initialize reader with the file url
void ORCFileSplitter::load(std::string url) {
    cur_fn_ = "";
    url_ = url;

    struct hdfsBuilder* builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, husky::Context::get_param("hdfs_namenode").c_str());
    hdfsBuilderSetNameNodePort(builder, std::stoi(husky::Context::get_param("hdfs_namenode_port")));
    fs_ = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
}

// ask master for offset and url
boost::string_ref ORCFileSplitter::fetch_batch() {
    BinStream question;
    question << url_;
    BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_ORC_BLK_REQ);
    std::string fn;
    size_t offset;
    answer >> fn;
    answer >> offset;
    if (fn == "") {
        return "";
    } else if (fn != cur_fn_) {
        cur_fn_ = fn;
        ReaderOptions opts;
        reader_ = createReader(read_hdfs_file(fs_, cur_fn_), opts);
    }
    return read_by_batch(offset);
}

boost::string_ref ORCFileSplitter::read_by_batch(size_t offset) {
    buffer_.clear();
    try {
        std::string line = "";
        reader_->seekToRow(offset);
        std::unique_ptr<orc::ColumnPrinter> printer(new SQLColumnPrinter(line, reader_->getSelectedType()));
        std::unique_ptr<ColumnVectorBatch> batch = reader_->createRowBatch(kOrcRowBatchSize);

        if (reader_->next(*batch)) {
            printer->reset(*batch);
            for (unsigned int i = 0; i < batch->numElements; ++i) {
                line.clear();
                printer->printRow(i);
                line += "\n";
                buffer_ += line;
            }
        }
    } catch (const std::exception& e) {
        LOG_I << e.what();
    }
    return boost::string_ref(buffer_);
}

}  // namespace io
}  // namespace husky

#endif
