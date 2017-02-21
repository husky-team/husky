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

#include <string>

#include "base/assert.hpp"
#include "base/log.hpp"

int main(int argc, char** argv) {
    husky::base::log_init(argv[0]);
    husky::base::log_to_dir("test-log-msg");
    husky::LOG_I << "Info Log";
    husky::DLOG_I << "Info Log in debug mode";
    husky::LOG_W << "Warning Log";
    husky::DLOG_W << "Warning Log in debug mode";
    husky::LOG_E << "Error Log";
    husky::DLOG_E << "Error Log in debug mode";
    //husky::LOG_F << "Fatal Log";
    //husky::DLOG_F << "Fatal Log";
    //husky::ASSERT(0);
    //husky::ASSERT_MSG(0, "abc");
    //husky::DASSERT(0);
    husky::DASSERT_MSG(0, "abc");
    return 0;
}
