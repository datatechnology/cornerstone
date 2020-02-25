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

#ifndef _RAFT_SERVER_HXX_
#define _RAFT_SERVER_HXX_

namespace cornerstone {
    class raft_server {
    public:
        raft_server(context* ctx);
        virtual ~raft_server();

    __nocopy__(raft_server)
    
    public:
        ptr<resp_msg> process_req(req_msg& req);

        ptr<async_result<bool>> add_srv(const srv_config& srv);

        ptr<async_result<bool>> remove_srv(const int srv_id);

        /**
         * appends log entries to cluster.
         * all log buffer pointers will be moved and become invalid after this returns.
         * this will automatically forward the request to leader if current node is not
         * the leader, so that no log entry cookie is allowed since the log entries may
         * be serialized and transfer to current leader.
         * @return async result that indicates the log entries have been accepted or not
         */
        ptr<async_result<bool>> append_entries(std::vector<bufptr>& logs);

        /**
         * replicates a log entry to cluster.
         * log buffer pointer will be moved and become invalid after this returns.
         * @return true if current node is the leader and log entry is accepted.
         */ 
        bool replicate_log(bufptr& log, const ptr<void>& cookie, uint cookie_tag);

        bool is_leader() const;

    private:
        typedef std::unordered_map<int32, ptr<peer>>::const_iterator peer_itor;

    private:
        ptr<resp_msg> handle_append_entries(req_msg& req);
        ptr<resp_msg> handle_vote_req(req_msg& req);
        ptr<resp_msg> handle_cli_req(req_msg& req);
        ptr<resp_msg> handle_extended_msg(req_msg& req);
        ptr<resp_msg> handle_install_snapshot_req(req_msg& req);
        ptr<resp_msg> handle_rm_srv_req(req_msg& req);
        ptr<resp_msg> handle_add_srv_req(req_msg& req);
        ptr<resp_msg> handle_log_sync_req(req_msg& req);
        ptr<resp_msg> handle_join_cluster_req(req_msg& req);
        ptr<resp_msg> handle_leave_cluster_req(req_msg& req);
        ptr<resp_msg> handle_prevote_req(req_msg& req);
        bool handle_snapshot_sync_req(snapshot_sync_req& req);
        void request_vote();
        void request_prevote();
        void request_append_entries();
        bool request_append_entries(peer& p);
        void handle_peer_resp(ptr<resp_msg>& resp, const ptr<rpc_exception>& err);
        void handle_append_entries_resp(resp_msg& resp);
        void handle_install_snapshot_resp(resp_msg& resp);
        void handle_voting_resp(resp_msg& resp);
        void handle_ext_resp(ptr<resp_msg>& resp, const ptr<rpc_exception>& err);
        void handle_ext_resp_err(rpc_exception& err);
        void handle_prevote_resp(resp_msg& resp);
        ptr<req_msg> create_append_entries_req(peer& p);
        ptr<req_msg> create_sync_snapshot_req(peer& p, ulong last_log_idx, ulong term, ulong commit_idx);
        void commit(ulong target_idx);
        void snapshot_and_compact(ulong committed_idx);
        bool update_term(ulong term);
        void reconfigure(const ptr<cluster_config>& new_config);
        void become_candidate();
        void become_leader();
        void become_follower();
        void enable_hb_for_peer(peer& p);
        void restart_election_timer();
        void stop_election_timer();
        void handle_hb_timeout(peer& peer);
        void handle_election_timeout();
        void sync_log_to_new_srv(ulong start_idx);
        void invite_srv_to_join_cluster();
        void rm_srv_from_cluster(int32 srv_id);
        int get_snapshot_sync_block_size() const;
        void on_snapshot_completed(ptr<snapshot>& s, bool result, const ptr<std::exception>& err);
        void on_retryable_req_err(ptr<peer>& p, ptr<req_msg>& req);
        ulong term_for_log(ulong log_idx);
        void commit_in_bg();
        ptr<async_result<bool>> send_msg_to_leader(ptr<req_msg>& req);
    private:
        static const int default_snapshot_sync_block_size;
        int32 leader_;
        int32 id_;
        int32 votes_granted_;
        ulong quick_commit_idx_;
        ulong sm_commit_index_;
        bool election_completed_;
        bool config_changing_;
        bool catching_up_;
        bool stopping_;
        int32 steps_to_down_;
        std::atomic_bool snp_in_progress_;
        std::unique_ptr<context> ctx_;
        ptr<delayed_task_scheduler> scheduler_;
        timer_task<void>::executor election_exec_;
        ptr<delayed_task> election_task_;
        std::unordered_map<int32, ptr<peer>> peers_;
        mutable std::shared_timed_mutex peers_lock_;
        std::unordered_map<int32, ptr<rpc_client>> rpc_clients_;
        srv_role role_;
        ptr<srv_state> state_;
        ptr<log_store> log_store_;
        ptr<state_machine> state_machine_;
        ptr<logger> l_;
        std::function<int32()> rand_timeout_;
        ptr<cluster_config> config_;
        ptr<peer> srv_to_join_;
        ptr<srv_config> conf_to_add_;
        std::recursive_mutex lock_;
        std::mutex commit_lock_;
        std::mutex rpc_clients_lock_;
        std::condition_variable commit_cv_;
        std::mutex stopping_lock_;
        std::condition_variable ready_to_stop_cv_;
        rpc_handler resp_handler_;
        rpc_handler ex_resp_handler_;
        ptr<snapshot> last_snapshot_;
        std::unordered_set<int32> voted_servers_;
        uptr<prevote_state> prevote_state_;
    };
}
#endif //_RAFT_SERVER_HXX_
