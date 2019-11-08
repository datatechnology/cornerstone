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

#ifndef _CONTEXT_HXX_
#define _CONTEXT_HXX_

namespace cornerstone {
    struct context {
    public:
        context(
            const ptr<state_mgr>& mgr,
            const ptr<state_machine>& m, 
            const ptr<rpc_listener>& listener, 
            const ptr<logger>& l, 
            const ptr<rpc_client_factory>& cli_factory, 
            const ptr<delayed_task_scheduler>& scheduler,
            const ptr<raft_event_listener>& event_listener,
            raft_params* params = nilptr)
            : state_mgr_(mgr), 
              state_machine_(m),
              rpc_listener_(listener), 
              logger_(l), 
              rpc_cli_factory_(cli_factory), 
              scheduler_(scheduler), 
              event_listener_(event_listener),
              params_(params == nilptr ? new raft_params : params) {}

    __nocopy__(context)
    public:
        ptr<state_mgr> state_mgr_;
        ptr<state_machine> state_machine_;
        ptr<rpc_listener> rpc_listener_;
        ptr<logger> logger_;
        ptr<rpc_client_factory> rpc_cli_factory_;
        ptr<delayed_task_scheduler> scheduler_;
        ptr<raft_event_listener> event_listener_;
        uptr<raft_params> params_;
    };
}

#endif //_CONTEXT_HXX_
