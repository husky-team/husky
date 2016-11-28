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

#pragma once

#include <algorithm>
#include <cmath>
#include <string>

#include "boost/tokenizer.hpp"

#include "core/executor.hpp"
#include "core/objlist.hpp"
#include "core/utils.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/ml/feature_label.hpp"
#include "lib/vector.hpp"

namespace husky {
namespace lib {
namespace ml {

// indicate format
enum DataFormat { kLIBSVMFormat, kTSVFormat };

// load data without knowing the number of features
template <typename FeatureT, typename LabelT, bool is_sparse>
int load_data(std::string url, ObjList<LabeledPointHObj<FeatureT, LabelT, is_sparse>>& data, DataFormat format) {
    using DataObj = LabeledPointHObj<FeatureT, LabelT, is_sparse>;

    auto& infmt = husky::io::InputFormatStore::create_line_inputformat();
    infmt.set_input(url);

    husky::lib::Aggregator<int> num_features_agg(0, [](int& a, const int& b) { a = std::max(a, b); });
    auto& ac = AggregatorFactory::get_channel();

    std::function<void(boost::string_ref)> parser;
    if (format == kLIBSVMFormat) {
        parser = [&](boost::string_ref chunk) {
            if (chunk.empty())
                return;
            boost::char_separator<char> sep(" \t");
            boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);

            // get the largest index of features for this record
            int sz = 0;
            if (!is_sparse) {
                auto last_colon = chunk.find_last_of(':');
                if (last_colon != -1) {
                    auto last_space = chunk.substr(0, last_colon).find_last_of(' ');
                    sz = std::stoi(chunk.substr(last_space + 1, last_colon).data());
                }
                ASSERT_MSG(sz > 0, "The input file does not conform to LibSVM format.");
            }
            DataObj this_obj(sz);  // create a data object

            bool is_y = true;
            for (auto& w : tok) {
                if (!is_y) {
                    boost::char_separator<char> sep2(":");
                    boost::tokenizer<boost::char_separator<char>> tok2(w, sep2);
                    auto it = tok2.begin();
                    int idx = std::stoi(*it++);
                    double val = std::stod(*it++);
                    num_features_agg.update(idx);
                    this_obj.x.set(idx - 1, val);
                } else {
                    this_obj.y = std::stod(w);
                    is_y = false;
                }
            }
            data.add_object(this_obj);
        };
    } else if (format == kTSVFormat) {
        parser = [&](boost::string_ref chunk) {
            if (chunk.empty())
                return;

            boost::char_separator<char> sep(" \t");
            boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);
            boost::tokenizer<boost::char_separator<char>> tok1(chunk, sep);

            // get the number of features
            int i = 0, num_features = -1;
            for (auto& w : tok) {
                num_features++;
            }

            DataObj this_obj(num_features);  // create a data object
            for (auto& w : tok1) {
                if (i < num_features) {
                    this_obj.x.set(i++, std::stod(w));
                } else {
                    this_obj.y = std::stod(w);
                }
            }
            data.add_object(this_obj);
            num_features_agg.update(num_features);
        };
    } else {
        ASSERT_MSG(false, "Unsupported data format");
    }
    husky::load(infmt, {&ac}, parser);

    int num_features = num_features_agg.get_value();
    list_execute(data, [&](DataObj& this_obj) {
        if (this_obj.x.get_feature_num() != num_features) {
            this_obj.x.resize(num_features);
        }
    });
    return num_features;
}

template <typename FeatureT, typename LabelT, bool is_sparse>
void load_data(std::string url, ObjList<LabeledPointHObj<FeatureT, LabelT, is_sparse>>& data, DataFormat format,
               int num_features) {
    ASSERT_MSG(num_features > 0, "the number of features is non-positive.");
    using DataObj = LabeledPointHObj<FeatureT, LabelT, is_sparse>;

    husky::io::LineInputFormat infmt;
    infmt.set_input(url);

    std::function<void(boost::string_ref)> parser;
    if (format == kLIBSVMFormat) {
        parser = [&](boost::string_ref chunk) {
            if (chunk.empty())
                return;
            boost::char_separator<char> sep(" \t");
            boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);

            DataObj this_obj(num_features);

            bool is_y = true;
            for (auto& w : tok) {
                if (!is_y) {
                    boost::char_separator<char> sep2(":");
                    boost::tokenizer<boost::char_separator<char>> tok2(w, sep2);
                    auto it = tok2.begin();
                    int idx = std::stoi(*it++) - 1;
                    double val = std::stod(*it++);
                    this_obj.x.set(idx, val);
                } else {
                    this_obj.y = std::stod(w);
                    is_y = false;
                }
            }
            data.add_object(this_obj);
        };
    } else if (format == kTSVFormat) {
        parser = [&](boost::string_ref chunk) {
            if (chunk.empty())
                return;
            boost::char_separator<char> sep(" \t");
            boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);

            DataObj this_obj(num_features);

            int i = 0;
            for (auto& w : tok) {
                if (i < num_features) {
                    this_obj.x.set(i++, std::stod(w));
                } else {
                    this_obj.y = std::stod(w);
                }
            }
            data.add_object(this_obj);
        };
    } else {
        ASSERT_MSG(false, "Unsupported data format");
    }
    husky::load(infmt, parser);
}

}  // namespace ml
}  // namespace lib
}  // namespace husky
