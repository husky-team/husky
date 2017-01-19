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

#include "boost/utility/string_ref.hpp"

namespace husky {
namespace io {
namespace helper {

/// A helper function to find the next 'c' starting from index 'l'
size_t find_next(boost::string_ref sref, size_t l, char c);

/// A helper function to find the last 'c' from the end
size_t find_last(boost::string_ref sref, char c);

/// A helper function to find the next 'str' starting from index 'l'
size_t find_next(boost::string_ref sref, size_t l, const std::string& str);

}  // namespace helper
}  // namespace io
}  // namespace husky
