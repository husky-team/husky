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

#include "core/channel/channel_base.hpp"
#include "core/channel/channel_source.hpp"
#include "core/objlist.hpp"

namespace husky {

template <typename DstObjT>
class Source2ObjListChannel : public ChannelBase {
   protected:
    Source2ObjListChannel(ChannelSource* src_ptr, ObjList<DstObjT>* dst_ptr) : src_ptr_(src_ptr), dst_ptr_(dst_ptr) {}

    ~Source2ObjListChannel() override = default;

    Source2ObjListChannel(const Source2ObjListChannel&) = delete;
    Source2ObjListChannel& operator=(const Source2ObjListChannel&) = delete;

    Source2ObjListChannel(Source2ObjListChannel&&) = default;
    Source2ObjListChannel& operator=(Source2ObjListChannel&&) = default;

    ChannelSource* src_ptr_ = nullptr;
    ObjList<DstObjT>* dst_ptr_ = nullptr;
};

template <typename SrcObjT, typename DstObjT>
class ObjList2ObjListChannel : public ChannelBase {
   protected:
    ObjList2ObjListChannel(ObjList<SrcObjT>* src_ptr, ObjList<DstObjT>* dst_ptr)
        : src_ptr_(src_ptr), dst_ptr_(dst_ptr) {}

    ~ObjList2ObjListChannel() override = default;

    ObjList2ObjListChannel(const ObjList2ObjListChannel&) = delete;
    ObjList2ObjListChannel& operator=(const ObjList2ObjListChannel&) = delete;

    ObjList2ObjListChannel(ObjList2ObjListChannel&&) = default;
    ObjList2ObjListChannel& operator=(ObjList2ObjListChannel&&) = default;

    ObjList<SrcObjT>* src_ptr_ = nullptr;
    ObjList<DstObjT>* dst_ptr_ = nullptr;
};

}  // namespace husky
