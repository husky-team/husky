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

#include <unordered_map>

namespace husky {

std::unordered_map<int, int> base_balance_algo(std::unordered_map<int, int>& num_objs, int id);

std::unordered_map<int, std::unordered_map<int, int>> base_balance_algo_for_all(std::unordered_map<int, int>& num_objs);

std::unordered_map<int, int> local_balance_first_algo(std::unordered_map<int, int>& num_objs, int id);

std::unordered_map<int, std::unordered_map<int, int>> local_balance_first_algo_for_all(
    std::unordered_map<int, int>& num_objs);
}  // namespace husky
