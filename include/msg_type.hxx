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

#ifndef _MESSAGE_TYPE_HXX_
#define _MESSAGE_TYPE_HXX_

namespace cornerstone{
    enum msg_type{
        vote_request = 0x1,
        vote_response,
        append_entries_request,
        append_entries_response,
        client_request,
        add_server_request,
        add_server_response,
        remove_server_request,
        remove_server_response,
        sync_log_request,
        sync_log_response,
        join_cluster_request,
        join_cluster_response,
        leave_cluster_request,
        leave_cluster_response,
        install_snapshot_request,
        install_snapshot_response,
        prevote_request,
        prevote_response
    };
}

#endif //_MESSAGE_TYPE_HXX_