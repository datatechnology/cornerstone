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

#ifndef _CORNERSTONE_HXX_
#define _CORNERSTONE_HXX_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#ifdef max
#undef max
#endif

#ifdef min
#undef min
#endif

#include "asio_service.hxx"
#include "async.hxx"
#include "basic_types.hxx"
#include "buffer.hxx"
#include "cluster_config.hxx"
#include "context.hxx"
#include "delayed_task.hxx"
#include "delayed_task_scheduler.hxx"
#include "events.hxx"
#include "fs_log_store.hxx"
#include "log_entry.hxx"
#include "log_store.hxx"
#include "log_val_type.hxx"
#include "logger.hxx"
#include "msg_base.hxx"
#include "msg_type.hxx"
#include "peer.hxx"
#include "pp_util.hxx"
#include "ptr.hxx"
#include "raft_params.hxx"
#include "raft_server.hxx"
#include "req_msg.hxx"
#include "resp_msg.hxx"
#include "rpc_cli.hxx"
#include "rpc_cli_factory.hxx"
#include "rpc_exception.hxx"
#include "rpc_listener.hxx"
#include "snapshot.hxx"
#include "snapshot_sync_ctx.hxx"
#include "snapshot_sync_req.hxx"
#include "srv_config.hxx"
#include "srv_role.hxx"
#include "srv_state.hxx"
#include "state_machine.hxx"
#include "state_mgr.hxx"
#include "strfmt.hxx"
#include "timer_task.hxx"
#endif // _CORNERSTONE_HXX_
