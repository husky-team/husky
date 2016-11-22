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

#include "shuffle_combiner_store.hpp"

namespace husky {

std::unordered_map<size_t, ShuffleCombinerSetBase*> ShuffleCombinerStore::shuffle_combiners_map;
std::unordered_map<size_t, size_t> ShuffleCombinerStore::num_local_threads;
std::mutex ShuffleCombinerStore::shuffle_combiners_map_mutex;

}  // namespace husky
