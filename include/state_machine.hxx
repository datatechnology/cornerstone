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

#ifndef _STATE_MACHINE_HXX_
#define _STATE_MACHINE_HXX_

namespace cornerstone {
    class state_machine {
    __interface_body__(state_machine)

    public:
        virtual void commit(const ulong log_idx, buffer& data, const uptr<log_entry_cookie>& cookie) = 0;
        virtual void pre_commit(const ulong log_idx, buffer& data, const uptr<log_entry_cookie>& cookie) = 0;
        virtual void rollback(const ulong log_idx, buffer& data, const uptr<log_entry_cookie>& cookie) = 0;
        virtual void save_snapshot_data(snapshot& s, const ulong offset, buffer& data) = 0;
        virtual bool apply_snapshot(snapshot& s) = 0;
        virtual int read_snapshot_data(snapshot& s, const ulong offset, buffer& data) = 0;
        virtual ptr<snapshot> last_snapshot() = 0;
        virtual ulong last_commit_index() = 0;
        virtual void create_snapshot(snapshot& s, async_result<bool>::handler_type& when_done) = 0;
    };
}

#endif //_STATE_MACHINE_HXX_
