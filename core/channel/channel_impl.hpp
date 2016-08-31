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
   public:
    Source2ObjListChannel(ChannelSource* src_ptr, ObjList<DstObjT>* dst_ptr) : src_ptr_(src_ptr), dst_ptr_(dst_ptr) {}

   protected:
    ChannelSource* src_ptr_ = nullptr;
    ObjList<DstObjT>* dst_ptr_ = nullptr;
};

template <typename SrcObjT, typename DstObjT>
class ObjList2ObjListChannel : public ChannelBase {
   public:
    ObjList2ObjListChannel(ObjList<SrcObjT>* src_ptr, ObjList<DstObjT>* dst_ptr)
        : src_ptr_(src_ptr), dst_ptr_(dst_ptr) {}

   protected:
    ObjList<SrcObjT>* src_ptr_ = nullptr;
    ObjList<DstObjT>* dst_ptr_ = nullptr;
};

}  // namespace husky
