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
#include <cassert>
#include <cmath>
#include <string>
#include <utility>
#include <vector>

#include "boost/tokenizer.hpp"
#include "core/engine.hpp"
#include "io/input/line_inputformat.hpp"
#include "lib/aggregator_factory.hpp"
#include "lib/ml/feature_label.hpp"

namespace husky {
namespace lib {
namespace ml {
typedef std::vector<double> vec_double;
typedef std::vector<std::pair<int, double>> vec_sp;

// indicate format
const int kLIBSVMFormat = 0;
const int kTSVFormat = 1;

template <typename ObjT = FeatureLabel>
class DataLoader {
    typedef ObjList<ObjT> ObjL;

   public:
    DataLoader() {}
    explicit DataLoader(int _format) { this->format_ = _format; }
    explicit DataLoader(std::function<void(std::string, ObjL&)> _load_func) { load_func_ = _load_func; }

    void load_info(std::string url, ObjL& data) {
        ASSERT_MSG(load_func_ != nullptr, "Load function is not specified.");
        load_func_(url, data);
    }

    inline int get_num_feature() const { return this->num_feature_; }

   protected:
    int num_feature_ = -1;
    int format_;
    std::function<void(std::string, ObjL&)> load_func_ = nullptr;
};

template <>
DataLoader<FeatureLabel>::DataLoader(int _format)
    : DataLoader([this](std::string url, DataLoader<FeatureLabel>::ObjL& data) {

          husky::io::LineInputFormat infmt;
          infmt.set_input(url);

          husky::lib::Aggregator<int> num_features_agg(0, [](int& a, const int& b) { a = b; });
          auto& ac = husky::lib::AggregatorFactory::get_channel();

          std::function<void(boost::string_ref)> parser;
          if (this->format_ == kLIBSVMFormat) {
              // LIBSVM format -- [label] [featureIndex]:[featureValue] [featureIndex]:[featureValue]
              parser = [&](boost::string_ref chunk) {
                  if (chunk.empty())
                      return;
                  boost::char_separator<char> sep(" \t");
                  boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);

                  FeatureLabel this_obj;
                  double& label = this_obj.use_label();
                  vec_double& feature = this_obj.use_feature();

                  int i = 0;
                  for (auto& w : tok) {
                      if (i++) {
                          boost::char_separator<char> sep2(":");
                          boost::tokenizer<boost::char_separator<char>> tok2(w, sep2);
                          auto it = tok2.begin();
                          int idx = std::stoi(*it++);
                          double val = std::stod(*it++);
                          num_features_agg.update(idx);
                          feature.push_back(val);
                      } else {
                          label = std::stod(w);
                      }
                  }
                  data.add_object(this_obj);
              };
          } else if (this->format_ == kTSVFormat) {
              // TSV format -- [feature]\t[feature]\t[label]
              parser = [&](boost::string_ref chunk) {
                  if (chunk.empty())
                      return;
                  boost::char_separator<char> sep(" \t");
                  boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);

                  FeatureLabel this_obj;
                  double& label = this_obj.use_label();
                  vec_double& feature = this_obj.use_feature();

                  int i = 0;
                  for (auto& w : tok) {
                      feature.push_back(std::stod(w));
                  }
                  label = feature.back();
                  feature.pop_back();
                  data.add_object(this_obj);
                  num_features_agg.update(feature.size());
              };
          } else {
              ASSERT_MSG(false, "Unsupported data format");
              // parser = [&](boost::string_ref chunk) {};
          }
          husky::load(infmt, {&ac}, parser);
          this->num_feature_ = std::max(this->num_feature_, num_features_agg.get_value());
          // husky::globalize(data);
      }) {
    this->format_ = _format;
}

template <>
DataLoader<SparseFeatureLabel>::DataLoader(int _format)
    : DataLoader([this](std::string url, DataLoader<SparseFeatureLabel>::ObjL& data) {
          husky::io::LineInputFormat infmt;
          infmt.set_input(url);

          husky::lib::Aggregator<int> num_features_agg(0, [](int& a, const int& b) {
              a = std::max(a, b);
          });
          auto& ac = husky::lib::AggregatorFactory::get_channel();
          std::function<void(boost::string_ref)> parser;
          if (this->format_ == kLIBSVMFormat) {
              parser = [&](boost::string_ref chunk) {
                  if (chunk.empty())
                      return;
                  boost::char_separator<char> sep(" \t");
                  boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);

                  SparseFeatureLabel this_obj;
                  double& label = this_obj.use_label();
                  vec_sp& feature = this_obj.use_feature();

                  int i = 0;
                  for (auto& w : tok) {
                      if (i++) {
                          boost::char_separator<char> sep2(":");
                          boost::tokenizer<boost::char_separator<char>> tok2(w, sep2);
                          auto it = tok2.begin();
                          int idx = std::stoi(*it++);
                          double val = std::stod(*it++);
                          num_features_agg.update(idx);
                          feature.push_back(std::make_pair(idx, val));
                      } else {
                          label = std::stod(w);
                      }
                  }
                  data.add_object(this_obj);
              };
          } else if (this->format_ == kTSVFormat) {
              parser = [&](boost::string_ref chunk) {
                  if (chunk.empty())
                      return;
                  boost::char_separator<char> sep(" \t");
                  boost::tokenizer<boost::char_separator<char>> tok(chunk, sep);

                  SparseFeatureLabel this_obj;
                  double& label = this_obj.use_label();
                  vec_sp& feature = this_obj.use_feature();

                  int i = 0;
                  for (auto& w : tok) {
                      i++;
                      feature.push_back(std::make_pair(i, std::stod(w)));
                  }
                  label = feature.back().second;
                  feature.pop_back();
                  data.add_object(this_obj);
                  num_features_agg.update(feature.size());
              };
          } else {
              ASSERT_MSG(false, "Unsupported data format");
              // parser = [&](boost::string_ref chunk) {};
          }
          husky::load(infmt, {&ac}, parser);
          this->num_feature_ = std::max(this->num_feature_, num_features_agg.get_value());
      }) {
    this->format_ = _format;
}

}  // namespace ml
}  // namespace lib
}  // namespace husky
