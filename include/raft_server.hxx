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
        raft_server(context* ctx)
        : leader_(-1),
            id_(ctx->state_mgr_->server_id()),
            votes_granted_(0),
            quick_commit_idx_(ctx->state_machine_->last_commit_index()),
            sm_commit_index_(ctx->state_machine_->last_commit_index()),
            election_completed_(true),
            config_changing_(false),
            catching_up_(false),
            stopping_(false),
            steps_to_down_(0),
            snp_in_progress_(),
            ctx_(ctx),
            scheduler_(ctx->scheduler_),
            election_exec_(std::bind(&raft_server::handle_election_timeout, this)),
            election_task_(),
            peers_(),
            rpc_clients_(),
            role_(srv_role::follower),
            state_(ctx->state_mgr_->read_state()),
            log_store_(ctx->state_mgr_->load_log_store()),
            state_machine_(ctx->state_machine_),
            l_(ctx->logger_),
            config_(ctx->state_mgr_->load_config()),
            srv_to_join_(),
            conf_to_add_(),
            lock_(),
            commit_lock_(),
            rpc_clients_lock_(),
            commit_cv_(),
            stopping_lock_(),
            ready_to_stop_cv_(),
            resp_handler_((rpc_handler)std::bind(&raft_server::handle_peer_resp, this, std::placeholders::_1, std::placeholders::_2)),
            ex_resp_handler_((rpc_handler)std::bind(&raft_server::handle_ext_resp, this, std::placeholders::_1, std::placeholders::_2)), 
            last_snapshot_(ctx->state_machine_->last_snapshot()),
            voted_servers_() {
            std::random_device engine;
            std::uniform_int_distribution<int32> distribution(ctx->params_->election_timeout_lower_bound_, ctx->params_->election_timeout_upper_bound_);
            rand_timeout_ = [&distribution, &engine]() -> int32_t {
                return distribution(engine);
            };

            if (!state_) {
                state_ = cs_new<srv_state>();
                state_->set_term(0);
                state_->set_voted_for(-1);
            }

            /**
            * I found this implementation is also a victim of bug https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
            * As the implementation is based on Diego's thesis
            * Fix:
            * We should never load configurations that is not committed,
            *   this prevents an old server from replicating an obsoleted config to other servers
            * The prove is as below:
            * Assume S0 is the last committed server set for the old server A
            * |- EXITS Log l which has been committed but l !BELONGS TO A.logs =>  Vote(A) < Majority(S0)
            * In other words, we need to prove that A cannot be elected to leader if any logs/configs has been committed.
            * Case #1, There is no configuration change since S0, then it's obvious that Vote(A) < Majority(S0), see the core Algorithm
            * Case #2, There are one or more configuration changes since S0, then at the time of first configuration change was committed,
            *      there are at least Majority(S0 - 1) servers committed the configuration change
            *      Majority(S0 - 1) + Majority(S0) > S0 => Vote(A) < Majority(S0)
            * -|
            */
            for (ulong i = std::max(sm_commit_index_ + 1, log_store_->start_index()); i < log_store_->next_slot(); ++i) {
                ptr<log_entry> entry(log_store_->entry_at(i));
                if (entry->get_val_type() == log_val_type::conf) {
                    l_->info(sstrfmt("detect a configuration change that is not committed yet at index %llu").fmt(i));
                    config_changing_ = true;
                    break;
                }
            }

            std::list<ptr<srv_config>>& srvs(config_->get_servers());
            for (cluster_config::srv_itor it = srvs.begin(); it != srvs.end(); ++it) {
                if ((*it)->get_id() != id_) {
         	        timer_task<peer&>::executor exec = (timer_task<peer&>::executor)std::bind(&raft_server::handle_hb_timeout, this, std::placeholders::_1);
                    peers_.insert(std::make_pair((*it)->get_id(), cs_new<peer, ptr<srv_config>&, context&, timer_task<peer&>::executor&>(*it, *ctx_, exec)));
                }
            }

            std::thread commiting_thread = std::thread(std::bind(&raft_server::commit_in_bg, this));
            commiting_thread.detach();
            restart_election_timer();
            l_->debug(strfmt<30>("server %d started").fmt(id_));
        }

        virtual ~raft_server() {
            recur_lock(lock_);
            stopping_ = true;
            std::unique_lock<std::mutex> commit_lock(commit_lock_);
            commit_cv_.notify_all();
            std::unique_lock<std::mutex> lock(stopping_lock_);
            commit_lock.unlock();
            commit_lock.release();
            ready_to_stop_cv_.wait(lock);
            if (election_task_) {
                scheduler_->cancel(election_task_);
            }

            for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
                if (it->second->get_hb_task()) {
                    scheduler_->cancel(it->second->get_hb_task());
                }
            }
        }

    __nocopy__(raft_server)
    
    public:
        ptr<resp_msg> process_req(req_msg& req);

        ptr<async_result<bool>> add_srv(const srv_config& srv);

        ptr<async_result<bool>> remove_srv(const int srv_id);

        ptr<async_result<bool>> append_entries(std::vector<bufptr>& logs);

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
        bool handle_snapshot_sync_req(snapshot_sync_req& req);
        void request_vote();
        void request_append_entries();
        bool request_append_entries(peer& p);
        void handle_peer_resp(ptr<resp_msg>& resp, const ptr<rpc_exception>& err);
        void handle_append_entries_resp(resp_msg& resp);
        void handle_install_snapshot_resp(resp_msg& resp);
        void handle_voting_resp(resp_msg& resp);
        void handle_ext_resp(ptr<resp_msg>& resp, const ptr<rpc_exception>& err);
        void handle_ext_resp_err(rpc_exception& err);
        ptr<req_msg> create_append_entries_req(peer& p);
        ptr<req_msg> create_sync_snapshot_req(peer& p, ulong last_log_idx, ulong term, ulong commit_idx);
        void commit(ulong target_idx);
        void snapshot_and_compact(ulong committed_idx);
        bool update_term(ulong term);
        void reconfigure(const ptr<cluster_config>& new_config);
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
    };
}
#endif //_RAFT_SERVER_HXX_
