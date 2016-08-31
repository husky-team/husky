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

#include <functional>
#include <string>
#include <utility>

namespace std {

template <>
class hash<pair<int, int>> {
   public:
    size_t operator()(const pair<int, int>& x) const;
};

template <>
class hash<pair<string, string>> {
   public:
    size_t operator()(const pair<string, string>& x) const;
};

}  // namespace std
