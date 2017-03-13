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

namespace husky {

// increase it if there are more workers
const int MAX_NUM_WORKERS = 500;
const int MAX_NUM_LOCAL_WORKERS = 100;

// async migration
const int MIGRATE_BUFFER_THRESHOLD = 3500;

//
// below are magic numbers.
//

// Communication type
const uint32_t COMM_PUSH = 0;
const uint32_t COMM_REQ = 1;
const uint32_t COMM_REP = 2;
const uint32_t COMM_MIGR = 3;
const uint32_t COMM_END = 4;
const uint32_t COMM_PRGS = 5;
const uint32_t COMM_PROBE = 6;

/// Mailbox
const uint32_t MAILBOX_EVENT_RECV_COMM = 0x2f3b1a66;
const uint32_t MAILBOX_EVENT_RECV_COMM_PRGS = 0x303b1366;
const uint32_t MAILBOX_EVENT_RECV_COMM_END = 0x30e31266;
const uint32_t MAILBOX_EVENT_RECV_COMM_PUSH = (COMM_PUSH << 2);
const uint32_t MAILBOX_EVENT_RECV_COMM_MIGR = (COMM_MIGR << 2);
const uint32_t MAILBOX_EVENT_RECV_COMM_REP = (COMM_REP << 2);
const uint32_t MAILBOX_EVENT_RECV_COMM_REQ = (COMM_REQ << 2);
const uint32_t MAILBOX_EVENT_SEND_COMM = 0x30eb1266;
const uint32_t MAILBOX_EVENT_SEND_COMM_PRGS = 0x33eb1266;
const uint32_t MAILBOX_EVENT_SEND_COMM_END = 0x303b1266;
const uint32_t MAILBOX_EVENT_DESTROY = 0x303b1276;

/// coordinate with master
const uint32_t TYPE_ECHO = 0x4543484f;
const uint32_t TYPE_HBASE = 0x45a1245f;
const uint32_t TYPE_HBASE_STATUS = 0x45b1245f;
const uint32_t TYPE_SESSION_BEGIN_PY = 0x3bfa1c58;
const uint32_t TYPE_SESSION_END_PY = 0x2dfb1c58;
const uint32_t TYPE_NEW_TASK = 0x30fa1258;
const uint32_t TYPE_QUERY_TASK = 0x40fa1257;
const uint32_t TYPE_REQ_INSTR = 0x4bfcb458;
const uint32_t TYPE_TASK_END = 0x34acd4cf;
const uint32_t TYPE_TASK_RES = 0x54524553;
const uint32_t TYPE_FLUSH_REQ = 0x30eb1266;
const uint32_t TYPE_FLUSH_YES = 0xfc939015;
const uint32_t TYPE_FLUSH_NO = 0x35253cf5;
const uint32_t TYPE_INC_SEQ = 0x65321f00;
const uint32_t TYPE_HDFS_REQ = 0xbd001076;
const uint32_t TYPE_HDFS_BLK_REQ = 0xfa0913d6;
const uint32_t TYPE_KAFKA_REQ = 0xfa091343;
const uint32_t TYPE_KAFKA_END_REQ = 0xfa091344;
const uint32_t TYPE_MONGODB_REQ = 0xfa091388;
const uint32_t TYPE_MONGODB_END_REQ = 0xfa091389;
const uint32_t TYPE_LOCAL_BLK_REQ = 0xfa0e12a2;
const uint32_t TYPE_ORC_BLK_REQ = 0xfa2e32a1;
const uint32_t TYPE_STOP_ASYNC_REQ = 0xf89d74b4;
const uint32_t TYPE_STOP_ASYNC_YES = 0x09b8ab2b;
const uint32_t TYPE_STOP_ASYNC_NO = 0x192a241a;
const uint32_t TYPE_START_ASYNC_REQ = 0x302233da;
const uint32_t TYPE_START_ASYNC_YES = 0x47d67f00;
const uint32_t TYPE_START_ASYNC_NO = 0x5c229448;
const uint32_t TYPE_HEARTBEAT = 0xcf99ed5e;
const uint32_t TYPE_PATCH_REQ = 0xf56c43c1;
const uint32_t TYPE_TIME = 0x29241ad6;
const uint32_t TYPE_JOIN = 0x47d69ed5;
const uint32_t TYPE_EXIT = 0x47d79fd5;
const uint32_t TYPE_GET_HASH_RING = 0x48d693d5;
const uint32_t TYPE_NFS_FILE_REQ = 0x4E465251;
const uint32_t TYPE_HDFS_FILE_REQ = 0x48465251;

}  // namespace husky
