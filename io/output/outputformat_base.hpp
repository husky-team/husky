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

#include "core/channel/channel_destination.hpp"

namespace husky {
namespace io {

/// Interface for OutputFormat.
class OutputFormatBase : public husky::ChannelDestination {
   public:
    virtual ~OutputFormatBase() = default;
    /// Check if all configures are set up.
    virtual bool is_setup() const = 0;

   protected:
    int is_setup_ = 0;
};

}  // namespace io
}  // namespace husky
