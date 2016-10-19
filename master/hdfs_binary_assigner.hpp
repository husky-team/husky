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

#include <string>
#include <unordered_map>
#include <vector>

#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "master/master.hpp"

namespace husky {

struct HDFSFileInfo {
    std::vector<std::string> files_to_assign;
    int finish_count{0};
    std::string base{""};
};

class HDFSFileAssigner {
   public:
    HDFSFileAssigner();
    void setup();
    void response();

   private:
    std::string answer(const std::string& fileurl);
    void prepare(const std::string& url);

    std::unordered_map<std::string, HDFSFileInfo> file_infos_;
};

}  // namespace husky
