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

#ifndef _STATE_MGR_HXX_
#define _STATE_MGR_HXX_

namespace cornerstone {
    class state_mgr {
    __interface_body__(state_mgr)

    public:
        virtual ptr<cluster_config> load_config() = 0;
        virtual void save_config(const cluster_config& config) = 0;
        virtual void save_state(const srv_state& state) = 0;
        virtual ptr<srv_state> read_state() = 0;
        virtual ptr<log_store> load_log_store() = 0;
        virtual int32 server_id() = 0;
        virtual void system_exit(const int exit_code) = 0;
    };
}

#endif //_STATE_MGR_HXX_