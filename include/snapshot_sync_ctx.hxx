/**
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  The ASF licenses
* this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#ifndef _SNAPSHOT_SYNC_CTX_HXX_
#define _SNAPSHOT_SYNC_CTX_HXX_

namespace cornerstone {
    struct snapshot_sync_ctx {
    public:
        snapshot_sync_ctx(const ptr<snapshot>& s, ulong offset = 0L)
            : snapshot_(s), offset_(offset) {}

    __nocopy__(snapshot_sync_ctx)
    
    public:
        const ptr<snapshot>& get_snapshot() const {
            return snapshot_;
        }

        ulong get_offset() const {
            return offset_;
        }

        void set_offset(ulong offset) {
            offset_ = offset;
        }
    public:
        ptr<snapshot> snapshot_;
        ulong offset_;
    };
}

#endif //_SNAPSHOT_SYNC_CTX_HXX_
