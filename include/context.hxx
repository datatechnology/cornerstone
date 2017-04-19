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
            ptr<state_mgr>& mgr,
            ptr<state_machine>& m, 
            ptr<rpc_listener>& listener, 
            ptr<logger>& l, 
            ptr<rpc_client_factory>& cli_factory, 
            ptr<delayed_task_scheduler>& scheduler,
            raft_params* params = nilptr)
            : state_mgr_(mgr), 
              state_machine_(m),
              rpc_listener_(listener), 
              logger_(l), 
              rpc_cli_factory_(cli_factory), 
              scheduler_(scheduler), 
              params_(params == nilptr ? new raft_params : params) {}

    __nocopy__(context)
    public:
        ptr<state_mgr> state_mgr_;
        ptr<state_machine> state_machine_;
        ptr<rpc_listener> rpc_listener_;
        ptr<logger> logger_;
        ptr<rpc_client_factory> rpc_cli_factory_;
        ptr<delayed_task_scheduler> scheduler_;
        std::unique_ptr<raft_params> params_;
    };
}

#endif //_CONTEXT_HXX_
