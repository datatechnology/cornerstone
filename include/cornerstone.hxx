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

#ifndef _CORNERSTONE_HXX_
#define _CORNERSTONE_HXX_

#include <cstdio>
#include <cstdlib>
#include <cinttypes>
#include <cstring>
#include <memory>
#include <vector>
#include <list>
#include <string>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <exception>
#include <atomic>
#include <algorithm>
#include <unordered_map>
#include <random>
#include <chrono>
#include <thread>
#include <fstream>

#ifdef max
#undef max
#endif

#ifdef min
#undef min
#endif

#include "pp_util.hxx"
#include "strfmt.hxx"
#include "basic_types.hxx"
#include "ptr.hxx"
#include "raft_params.hxx"
#include "msg_type.hxx"
#include "buffer.hxx"
#include "log_val_type.hxx"
#include "log_entry.hxx"
#include "msg_base.hxx"
#include "req_msg.hxx"
#include "resp_msg.hxx"
#include "rpc_exception.hxx"
#include "async.hxx"
#include "logger.hxx"
#include "srv_config.hxx"
#include "cluster_config.hxx"
#include "srv_state.hxx"
#include "srv_role.hxx"
#include "log_store.hxx"
#include "state_mgr.hxx"
#include "rpc_listener.hxx"
#include "snapshot.hxx"
#include "state_machine.hxx"
#include "rpc_cli.hxx"
#include "rpc_cli_factory.hxx"
#include "delayed_task.hxx"
#include "timer_task.hxx"
#include "delayed_task_scheduler.hxx"
#include "context.hxx"
#include "snapshot_sync_ctx.hxx"
#include "snapshot_sync_req.hxx"
#include "peer.hxx"
#include "raft_server.hxx"
#include "asio_service.hxx"
#include "fs_log_store.hxx"
#endif // _CORNERSTONE_HXX_
