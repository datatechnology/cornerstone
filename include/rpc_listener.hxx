/*
 * Copyright (c) 2016 - present Alpha Infra Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _RPC_LISTENER_HXX_
#define _RPC_LISTENER_HXX_

#include "pp_util.hxx"
#include "ptr.hxx"

namespace cornerstone
{
// for backward compatibility
class raft_server;
typedef raft_server msg_handler;

class rpc_listener
{
    __interface_body__(rpc_listener);

public:
    virtual void listen(ptr<msg_handler>& handler) = 0;
    virtual void stop() = 0;
};
} // namespace cornerstone

#endif //_RPC_LISTENER_HXX_