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

#include "../include/cornerstone.hxx"

using namespace cornerstone;

const int raft_server::default_snapshot_sync_block_size = 4 * 1024;

// for tracing and debugging
static const char* __msg_type_str[] = {
    "unknown",
    "request_vote_request",
    "request_vote_response",
    "append_entries_request",
    "append_entries_response",
    "client_request",
    "add_server_request",
    "add_server_response",
    "remove_server_request",
    "remove_server_response",
    "sync_log_request",
    "sync_log_response",
    "join_cluster_request",
    "join_cluster_response",
    "leave_cluster_request",
    "leave_cluster_response",
    "install_snapshot_request",
    "install_snapshot_response"
};

ptr<resp_msg> raft_server::process_req(req_msg& req) {
    recur_lock(lock_);
    l_.debug(
        lstrfmt("Receive a %s message from %d with LastLogIndex=%llu, LastLogTerm=%llu, EntriesLength=%d, CommitIndex=%llu and Term=%llu")
        .fmt(
            __msg_type_str[req.get_type()],
            req.get_src(),
            req.get_last_log_idx(),
            req.get_last_log_term(),
            req.log_entries().size(),
            req.get_commit_idx(),
            req.get_term()));
    if (req.get_type() == msg_type::append_entries_request ||
        req.get_type() == msg_type::request_vote_request ||
        req.get_type() == msg_type::install_snapshot_request) {
        // we allow the server to be continue after term updated to save a round message
        update_term(req.get_term());

        // Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
        if (steps_to_down_ > 0) {
            steps_to_down_ = 2;
        }
    }

    ptr<resp_msg> resp;
    if (req.get_type() == msg_type::append_entries_request) {
        resp = handle_append_entries(req);
    }
    else if (req.get_type() == msg_type::request_vote_request) {
        resp = handle_vote_req(req);
    }
    else if (req.get_type() == msg_type::client_request) {
        resp = handle_cli_req(req);
    }
    else {
        // extended requests
        resp = handle_extended_msg(req);
    }

    if (resp) {
        l_.debug(
            lstrfmt("Response back a %s message to %d with Accepted=%d, Term=%llu, NextIndex=%llu")
            .fmt(
                __msg_type_str[resp->get_type()],
                resp->get_dst(),
                resp->get_accepted() ? 1 : 0,
                resp->get_term(),
                resp->get_next_idx()));
    }

    return resp;
}

ptr<resp_msg> raft_server::handle_append_entries(req_msg& req) {
    if (req.get_term() == state_->get_term()) {
        if (role_ == srv_role::candidate) {
            become_follower();
        }
        else if (role_ == srv_role::leader) {
            l_.debug(
                lstrfmt("Receive AppendEntriesRequest from another leader(%d) with same term, there must be a bug, server exits")
                .fmt(req.get_src()));
            ctx_->state_mgr_.system_exit(-1);
            ::exit(-1);
        }
        else {
            restart_election_timer();
        }
    }

    // After a snapshot the req.get_last_log_idx() may less than log_store_->next_slot() but equals to log_store_->next_slot() -1
    // In this case, log is Okay if req.get_last_log_idx() == lastSnapshot.get_last_log_idx() && req.get_last_log_term() == lastSnapshot.get_last_log_term()
    // In not accepted case, we will return log_store_->next_slot() for the leader to quick jump to the index that might aligned
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::append_entries_response, id_, req.get_src(), log_store_->next_slot()));
    bool log_okay = req.get_last_log_idx() == 0 ||
        (req.get_last_log_idx() < log_store_->next_slot() && req.get_last_log_term() == term_for_log(req.get_last_log_idx()));
    if (req.get_term() < state_->get_term() || !log_okay) {
        return resp;
    }

    // follower & log is okay
    if (req.log_entries().size() > 0) {
        // write logs to store, start from overlapped logs
        ulong idx = req.get_last_log_idx() + 1;
        size_t log_idx = 0;
        while (idx < log_store_->next_slot() && log_idx < req.log_entries().size()) {
            if (log_store_->term_at(idx) == req.log_entries().at(log_idx)->get_term()) {
                idx++;
                log_idx++;
            }
            else {
                break;
            }
        }

        // dealing with overwrites
        while (idx < log_store_->next_slot() && log_idx < req.log_entries().size()) {
            ptr<log_entry> old_entry(log_store_->entry_at(idx));
            if (old_entry->get_val_type() == log_val_type::app_log) {
                state_machine_.rollback(idx, old_entry->get_buf());
            }
            else if (old_entry->get_val_type() == log_val_type::conf) {
                l_.info(sstrfmt("revert from a prev config change to config at %llu").fmt(config_->get_log_idx()));
                config_changing_ = false;
            }

            log_store_->write_at(idx++, req.log_entries().at(log_idx++));
        }

        // append new log entries
        while (log_idx < req.log_entries().size()) {
            ptr<log_entry> entry = req.log_entries().at(log_idx ++);
            ulong idx_for_entry = log_store_->append(entry);
            if (entry->get_val_type() == log_val_type::conf) {
                l_.info(sstrfmt("receive a config change from leader at %llu").fmt(idx_for_entry));
                config_changing_ = true;

            }
            else {
                state_machine_.pre_commit(idx_for_entry, entry->get_buf());
            }
        }
    }

    leader_ = req.get_src();
    commit(req.get_commit_idx());
    resp->accept(req.get_last_log_idx() + req.log_entries().size() + 1);
    return resp;
}

ptr<resp_msg> raft_server::handle_vote_req(req_msg& req) {
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::request_vote_response, id_, req.get_src()));
    bool log_okay = req.get_last_log_term() > log_store_->last_entry()->get_term() ||
        (req.get_last_log_term() == log_store_->last_entry()->get_term() &&
            log_store_->next_slot() - 1 <= req.get_last_log_idx());
    bool grant = req.get_term() == state_->get_term() && log_okay && (state_->get_voted_for() == req.get_src() || state_->get_voted_for() == -1);
    if (grant) {
        resp->accept(log_store_->next_slot());
        state_->set_voted_for(req.get_src());
        ctx_->state_mgr_.save_state(*state_);
    }

    return resp;
}

ptr<resp_msg> raft_server::handle_cli_req(req_msg& req) {
    ptr<resp_msg> resp (cs_new<resp_msg>(state_->get_term(), msg_type::append_entries_response, id_, leader_));
    if (role_ != srv_role::leader) {
        return resp;
    }

    std::vector<ptr<log_entry>>& entries = req.log_entries();
    for (size_t i = 0; i < entries.size(); ++i) {
        log_store_->append(entries.at(i));
        state_machine_.pre_commit(log_store_->next_slot() - 1, entries.at(i)->get_buf());
    }

    // urgent commit, so that the commit will not depend on hb
    request_append_entries();
    resp->accept(log_store_->next_slot());
    return resp;
}

void raft_server::handle_election_timeout() {
    recur_lock(lock_);
    if (steps_to_down_ > 0) {
        if (--steps_to_down_ == 0) {
            l_.info("no hearing further news from leader, remove this server from cluster and step down");
            for (std::list<ptr<srv_config>>::iterator it = config_->get_servers().begin();
                it != config_->get_servers().end();
                ++it) {
                if ((*it)->get_id() == id_) {
                    config_->get_servers().erase(it);
                    ctx_->state_mgr_.save_config(*config_);
                    break;
                }
            }

            ctx_->state_mgr_.system_exit(-1);
            ::exit(0);
            return;
        }

        l_.info(sstrfmt("stepping down (cycles left: %d), skip this election timeout event").fmt(steps_to_down_));
        restart_election_timer();
        return;
    }

    if (catching_up_) {
        // this is a new server for the cluster, will not send out vote req until conf that includes this srv is committed
        l_.info("election timeout while joining the cluster, ignore it.");
        restart_election_timer();
        return;
    }

    if (role_ == srv_role::leader) {
        l_.err("A leader should never encounter election timeout, illegal application state, stop the application");
        ctx_->state_mgr_.system_exit(-1);
        ::exit(-1);
        return;
    }

    l_.debug("Election timeout, change to Candidate");
    state_->inc_term();
    state_->set_voted_for(-1);
    role_ = srv_role::candidate;
    votes_granted_ = 0;
    votes_responded_ = 0;
    election_completed_ = false;
    ctx_->state_mgr_.save_state(*state_);
    request_vote();

    // restart the election timer if this is not yet a leader
    if (role_ != srv_role::leader) {
        restart_election_timer();
    }
}

void raft_server::request_vote() {
    l_.info(sstrfmt("requestVote started with term %llu").fmt(state_->get_term()));
    state_->set_voted_for(id_);
    ctx_->state_mgr_.save_state(*state_);
    votes_granted_ += 1;
    votes_responded_ += 1;

    // is this the only server?
    if (votes_granted_ > (int32)(peers_.size() + 1) / 2) {
        election_completed_ = true;
        become_leader();
        return;
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        ptr<req_msg> req(cs_new<req_msg>(state_->get_term(), msg_type::request_vote_request, id_, it->second->get_id(), term_for_log(log_store_->next_slot() - 1), log_store_->next_slot() - 1, state_->get_commit_idx()));
        l_.debug(sstrfmt("send %s to server %d with term %llu").fmt(__msg_type_str[req->get_type()], it->second->get_id(), state_->get_term()));
        it->second->send_req(req, resp_handler_);
    }
}

void raft_server::request_append_entries() {
    if (peers_.size() == 0) {
        commit(log_store_->next_slot() - 1);
        return;
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        request_append_entries(*it->second);
    }
}

bool raft_server::request_append_entries(peer& p) {
    if (p.make_busy()) {
        ptr<req_msg> msg = create_append_entries_req(p);
        p.send_req(msg, resp_handler_);
        return true;
    }

    l_.debug(sstrfmt("Server %d is busy, skip the request").fmt(p.get_id()));
    return false;
}

void raft_server::handle_peer_resp(ptr<resp_msg>& resp, ptr<rpc_exception>& err) {
    recur_lock(lock_);
    if (err) {
        l_.info(sstrfmt("peer response error: %s").fmt(err->what()));
        return;
    }

    l_.debug(
        lstrfmt("Receive a %s message from peer %d with Result=%d, Term=%llu, NextIndex=%llu")
        .fmt(__msg_type_str[resp->get_type()], resp->get_src(), resp->get_accepted() ? 1 : 0, resp->get_term(), resp->get_next_idx()));

    // if term is updated, no more action is required
    if (update_term(resp->get_term())) {
        return;
    }

    // ignore the response that with lower term for safety
    switch (resp->get_type())
    {
    case msg_type::request_vote_response:
        handle_voting_resp(*resp);
        break;
    case msg_type::append_entries_response:
        handle_append_entries_resp(*resp);
        break;
    case msg_type::install_snapshot_response:
        handle_install_snapshot_resp(*resp);
        break;
    default:
        l_.err(sstrfmt("Received an unexpected message %s for response, system exits.").fmt(__msg_type_str[resp->get_type()]));
        ctx_->state_mgr_.system_exit(-1);
        ::exit(-1);
        break;
    }
}

void raft_server::handle_append_entries_resp(resp_msg& resp) {
    peer_itor it = peers_.find(resp.get_src());
    if (it == peers_.end()) {
        l_.info(sstrfmt("the response is from an unkonw peer %d").fmt(resp.get_src()));
        return;
    }

    // if there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
    bool need_to_catchup = true;
    ptr<peer> p = it->second;
    if (resp.get_accepted()) {
        {
            std::lock_guard<std::mutex>(p->get_lock());
            p->set_next_log_idx(resp.get_next_idx());
            p->set_matched_idx(resp.get_next_idx() - 1);
        }

        // try to commit with this response
        // TODO: keep this to save a "new" operation for each response
        std::unique_ptr<ulong> matched_indexes(new ulong[peers_.size() + 1]);
        matched_indexes.get()[0] = log_store_->next_slot() - 1;
        int i = 1;
        for (it = peers_.begin(); it != peers_.end(); ++it, ++i) {
            matched_indexes.get()[i] = it->second->get_matched_idx();
        }

        std::sort(matched_indexes.get(), matched_indexes.get() + (peers_.size() + 1), std::greater<ulong>());
        commit(matched_indexes.get()[(peers_.size() + 1) / 2]);
        need_to_catchup = p->clear_pending_commit() || resp.get_next_idx() < log_store_->next_slot();
    }
    else {
        std::lock_guard<std::mutex> guard(p->get_lock());
        if (resp.get_next_idx() > 0 && p->get_next_log_idx() > resp.get_next_idx()) {
            // fast move for the peer to catch up
            p->set_next_log_idx(resp.get_next_idx());
        }
        else {
            p->set_next_log_idx(p->get_next_log_idx() - 1);
        }
    }

    // This may not be a leader anymore, such as the response was sent out long time ago
    // and the role was updated by UpdateTerm call
    // Try to match up the logs for this peer
    if (role_ == srv_role::leader && need_to_catchup) {
        request_append_entries(*p);
    }
}

void raft_server::handle_install_snapshot_resp(resp_msg& resp) {
    peer_itor it = peers_.find(resp.get_src());
    if (it == peers_.end()) {
        l_.info(sstrfmt("the response is from an unkonw peer %d").fmt(resp.get_src()));
        return;
    }

    // if there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
    bool need_to_catchup = true;
    ptr<peer> p = it->second;
    if (resp.get_accepted()) {
        std::lock_guard<std::mutex> guard(p->get_lock());
        ptr<snapshot_sync_ctx> sync_ctx = p->get_snapshot_sync_ctx();
        if (sync_ctx == nilptr) {
            l_.info("no snapshot sync context for this peer, drop the response");
            need_to_catchup = false;
        }
        else {
            if (resp.get_next_idx() >= sync_ctx->get_snapshot()->size()) {
                l_.debug("snapshot sync is done");
                ptr<snapshot> nil_snp;
                p->set_next_log_idx(sync_ctx->get_snapshot()->get_last_log_idx() + 1);
                p->set_matched_idx(sync_ctx->get_snapshot()->get_last_log_idx());
                p->set_snapshot_in_sync(nil_snp);
                need_to_catchup = p->clear_pending_commit() || resp.get_next_idx() < log_store_->next_slot();
            }
            else {
                l_.debug(sstrfmt("continue to sync snapshot at offset %llu").fmt(resp.get_next_idx()));
                sync_ctx->set_offset(resp.get_next_idx());
            }
        }
    }
    else {
        l_.info("peer declines to install the snapshot, will retry");
    }

    // This may not be a leader anymore, such as the response was sent out long time ago
    // and the role was updated by UpdateTerm call
    // Try to match up the logs for this peer
    if (role_ == srv_role::leader && need_to_catchup) {
        request_append_entries(*p);
    }
}

void raft_server::handle_voting_resp(resp_msg& resp) {
    votes_responded_ += 1;
    if (election_completed_) {
        l_.info("Election completed, will ignore the voting result from this server");
        return;
    }

    if (resp.get_accepted()) {
        votes_granted_ += 1;
    }

    if (votes_responded_ >= (int32)(peers_.size() + 1)) {
        election_completed_ = true;
    }

    if (votes_granted_ > (int32)((peers_.size() + 1) / 2)) {
        l_.info(sstrfmt("Server is elected as leader for term %llu").fmt(state_->get_term()));
        election_completed_ = true;
        become_leader();
    }
}

void raft_server::handle_hb_timeout(peer& p) {
    recur_lock(lock_);
    l_.debug(sstrfmt("Heartbeat timeout for %d").fmt(p.get_id()));
    if (role_ == srv_role::leader) {
        request_append_entries(p);
        {
            std::lock_guard<std::mutex> guard(p.get_lock());
            if (p.is_hb_enabled()) {
                // Schedule another heartbeat if heartbeat is still enabled 
                ctx_->scheduler_.schedule(p.get_hb_task(), p.get_current_hb_interval());
            }
            else {
                l_.debug(sstrfmt("heartbeat is disabled for peer %d").fmt(p.get_id()));
            }
        }
    }
    else {
        l_.info(sstrfmt("Receive a heartbeat event for %d while no longer as a leader").fmt(p.get_id()));
    }
}

void raft_server::restart_election_timer() {
    // don't start the election timer while this server is still catching up the logs
    if (catching_up_) {
        return;
    }

    if (election_task_) {
        scheduler_.cancel(election_task_);
    }
    else {
        election_task_ = cs_new<timer_task<void>>(election_exec_);

    }

    scheduler_.schedule(election_task_, rand_timeout_());
}

void raft_server::stop_election_timer() {
    if (!election_task_) {
        l_.warn("Election Timer is never started but is requested to stop, protential a bug");
        return;
    }

    scheduler_.cancel(election_task_);
}

void raft_server::become_leader() {
    stop_election_timer();
    role_ = srv_role::leader;
    leader_ = id_;
    srv_to_join_.reset();
    ptr<snapshot> nil_snp;
    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        it->second->set_next_log_idx(log_store_->next_slot());
        it->second->set_snapshot_in_sync(nil_snp);
        it->second->set_free();
        enable_hb_for_peer(*(it->second));
    }

    if (config_->get_log_idx() == 0) {
        config_->set_log_idx(log_store_->next_slot());
        ptr<buffer> conf_buf = config_->serialize();
        ptr<log_entry> entry(cs_new<log_entry>(state_->get_term(), conf_buf, log_val_type::conf));
        log_store_->append(entry);
        l_.info("save initial config to log store");
        config_changing_ = true;
    }

    request_append_entries();
}

void raft_server::enable_hb_for_peer(peer& p) {
    p.enable_hb(true);
    p.resume_hb_speed();
    scheduler_.schedule(p.get_hb_task(), p.get_current_hb_interval());
}

void raft_server::become_follower() {
    // stop hb for all peers
    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        it->second->enable_hb(false);
    }

    srv_to_join_.reset();
    role_ = srv_role::follower;
    restart_election_timer();
}

bool raft_server::update_term(ulong term) {
    if (term > state_->get_term()) {
        state_->set_term(term);
        state_->set_voted_for(-1);
        election_completed_ = false;
        votes_granted_ = 0;
        votes_responded_ = 0;
        ctx_->state_mgr_.save_state(*state_);
        become_follower();
        return true;
    }

    return false;
}

void raft_server::commit(ulong target_idx) {
    if (target_idx > quick_commit_idx_) {
        quick_commit_idx_ = target_idx;

        // if this is a leader notify peers to commit as well
        // for peers that are free, send the request, otherwise, set pending commit flag for that peer
        if (role_ == srv_role::leader) {
            for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
                if (!request_append_entries(*(it->second))) {
                    it->second->set_pending_commit();
                }
            }
        }
    }

    if (log_store_->next_slot() - 1 > state_->get_commit_idx() && quick_commit_idx_ > state_->get_commit_idx()) {
        commit_cv_.notify_one();
    }
}

void raft_server::snapshot_and_compact(ulong committed_idx) {
    bool snapshot_in_action = false;
    try {
        bool f = false;
        if (ctx_->params_->snapshot_distance_ > 0
            && (committed_idx - log_store_->start_index()) > (ulong)ctx_->params_->snapshot_distance_
            && snp_in_progress_.compare_exchange_strong(f, true)) {
            snapshot_in_action = true;
            ptr<snapshot> snp(state_machine_.last_snapshot());
            if (snp && (committed_idx - snp->get_last_log_idx()) < (ulong)ctx_->params_->snapshot_distance_) {
                l_.info(sstrfmt("a very recent snapshot is available at index %llu, will skip this one").fmt(snp->get_last_log_idx()));
                snp_in_progress_.store(false);
                snapshot_in_action = false;
            }
            else {
                l_.info(sstrfmt("creating a snapshot for index %llu").fmt(committed_idx));

                // get the latest configuration info
                ptr<cluster_config> conf(config_);
                while (conf->get_log_idx() > committed_idx && conf->get_prev_log_idx() >= log_store_->start_index()) {
                    ptr<log_entry> conf_log(log_store_->entry_at(conf->get_prev_log_idx()));
                    conf = cluster_config::deserialize(conf_log->get_buf());
                }

                if (conf->get_log_idx() > committed_idx &&
                    conf->get_prev_log_idx() > 0 &&
                    conf->get_prev_log_idx() < log_store_->start_index()) {
                    ptr<snapshot> s(state_machine_.last_snapshot());
                    if (!s) {
                        l_.err("No snapshot could be found while no configuration cannot be found in current committed logs, this is a system error, exiting");
                        ctx_->state_mgr_.system_exit(-1);
                        ::exit(-1);
                        return;
                    }

                    conf = s->get_last_config();
                }
                else if (conf->get_log_idx() > committed_idx && conf->get_prev_log_idx() == 0) {
                    l_.err("BUG!!! stop the system, there must be a configuration at index one");
                    ctx_->state_mgr_.system_exit(-1);
                    ::exit(-1);
                    return;
                }

                ulong idx_to_compact = committed_idx - 1;
                ulong log_term_to_compact = log_store_->term_at(idx_to_compact);
                ptr<snapshot> new_snapshot(cs_new<snapshot>(idx_to_compact, log_term_to_compact, conf));
                async_result<bool>::handler_type handler = (async_result<bool>::handler_type) std::bind(&raft_server::on_snapshot_completed, this, new_snapshot, std::placeholders::_1, std::placeholders::_2);
                state_machine_.create_snapshot(
                    *new_snapshot,
                    handler);
                snapshot_in_action = false;
            }
        }
    }
    catch (...) {
        l_.err(sstrfmt("failed to compact logs at index %llu due to errors").fmt(committed_idx));
        if (snapshot_in_action) {
            bool val = true;
            snp_in_progress_.compare_exchange_strong(val, false);
        }
    }
}

void raft_server::on_snapshot_completed(ptr<snapshot>& s, bool result, ptr<std::exception>& err) {
    do {
        if (err != nilptr) {
            l_.err(lstrfmt("failed to create a snapshot due to %s").fmt(err->what()));
            break;
        }

        if (!result) {
            l_.info("the state machine rejects to create the snapshot");
            break;
        }

        {
            recur_lock(lock_);
            l_.debug("snapshot created, compact the log store");
            log_store_->compact(s->get_last_log_idx());
        }
    } while (false);
    snp_in_progress_.store(false);
}

ptr<req_msg> raft_server::create_append_entries_req(peer& p) {
    ulong cur_nxt_idx(0L);
    ulong commit_idx(0L);
    ulong last_log_idx(0L);
    ulong term(0L);
    ulong starting_idx(1L);

    {
        recur_lock(lock_);
        starting_idx = log_store_->start_index();
        cur_nxt_idx = log_store_->next_slot();
        commit_idx = quick_commit_idx_;
        term = state_->get_term();
    }

    {
        std::lock_guard<std::mutex> guard(p.get_lock());
        if (p.get_next_log_idx() == 0L) {
            p.set_next_log_idx(cur_nxt_idx);
        }

        last_log_idx = p.get_next_log_idx() - 1;
    }

    if (last_log_idx >= cur_nxt_idx) {
        l_.err(sstrfmt("Peer's lastLogIndex is too large %llu v.s. %llu, server exits").fmt(last_log_idx, cur_nxt_idx));
        ctx_->state_mgr_.system_exit(-1);
        ::exit(-1);
        return ptr<req_msg>();
    }

    // for syncing the snapshots, for starting_idx - 1, we can check with last snapshot
    if (last_log_idx > 0 && last_log_idx < starting_idx - 1) {
        return create_sync_snapshot_req(p, last_log_idx, term, commit_idx);
    }

    ulong last_log_term = term_for_log(last_log_idx);
    ulong end_idx = std::min(cur_nxt_idx, last_log_idx + 1 + ctx_->params_->max_append_size_);
    ptr<std::vector<ptr<log_entry>>> log_entries((last_log_idx + 1) >= cur_nxt_idx ? ptr<std::vector<ptr<log_entry>>>() : log_store_->log_entries(last_log_idx + 1, end_idx));
    l_.debug(
        lstrfmt("An AppendEntries Request for %d with LastLogIndex=%llu, LastLogTerm=%llu, EntriesLength=%d, CommitIndex=%llu and Term=%llu")
        .fmt(
            p.get_id(),
            last_log_idx,
            last_log_term,
            log_entries ? log_entries->size() : 0,
            commit_idx,
            term));
    ptr<req_msg> req(cs_new<req_msg>(term, msg_type::append_entries_request, id_, p.get_id(), last_log_term, last_log_idx, commit_idx));
    std::vector<ptr<log_entry>>& v = req->log_entries();
    if (log_entries) {
        v.insert(v.end(), log_entries->begin(), log_entries->end());
    }

    return req;
}

void raft_server::reconfigure(const ptr<cluster_config>& new_config) {
    l_.debug(
        lstrfmt("system is reconfigured to have %d servers, last config index: %llu, this config index: %llu")
        .fmt(new_config->get_servers().size(), new_config->get_prev_log_idx(), new_config->get_log_idx()));

    // we only allow one server to be added or removed at a time
    std::vector<int32> srvs_removed;
    std::vector<ptr<srv_config>> srvs_added;
    std::list<ptr<srv_config>>& new_srvs(new_config->get_servers());
    for (std::list<ptr<srv_config>>::const_iterator it = new_srvs.begin(); it != new_srvs.end(); ++it) {
        peer_itor pit = peers_.find((*it)->get_id());
        if (pit == peers_.end() && id_ != (*it)->get_id()) {
            srvs_added.push_back(*it);
        }
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        if (!new_config->get_server(it->first)) {
            srvs_removed.push_back(it->first);
        }
    }

    if (!new_config->get_server(id_)) {
        srvs_removed.push_back(id_);
    }

    for (std::vector<ptr<srv_config>>::const_iterator it = srvs_added.begin(); it != srvs_added.end(); ++it) {
        ptr<srv_config> srv_added(*it);
        timer_task<peer&>::executor exec = (timer_task<peer&>::executor)std::bind(&raft_server::handle_hb_timeout, this, std::placeholders::_1);
        ptr<peer> p = cs_new<peer, srv_config&, context&, timer_task<peer&>::executor&>(*srv_added, *ctx_, exec);
        p->set_next_log_idx(log_store_->next_slot());
        peers_.insert(std::make_pair(srv_added->get_id(), p));
        l_.info(sstrfmt("server %d is added to cluster").fmt(srv_added->get_id()));
        if (role_ == srv_role::leader) {
            l_.info(sstrfmt("enable heartbeating for server %d").fmt(srv_added->get_id()));
            enable_hb_for_peer(*p);
            if (srv_to_join_ && srv_to_join_->get_id() == p->get_id()) {
                p->set_next_log_idx(srv_to_join_->get_next_log_idx());
                srv_to_join_.reset();
            }
        }
    }

    for (std::vector<int32>::const_iterator it = srvs_removed.begin(); it != srvs_removed.end(); ++it) {
        int32 srv_removed = *it;
        if (srv_removed == id_ && !catching_up_) {
            // this server is removed from cluster
            ctx_->state_mgr_.save_config(*new_config);
            l_.info("server has been removed, step down");
            ctx_->state_mgr_.system_exit(0);
            return;
        }

        peer_itor pit = peers_.find(srv_removed);
        if (pit != peers_.end()) {
            pit->second->enable_hb(false);
            peers_.erase(pit);
            l_.info(sstrfmt("server %d is removed from cluster").fmt(srv_removed));
        }
        else {
            l_.info(sstrfmt("peer %d cannot be found, no action for removing").fmt(srv_removed));
        }
    }

    config_ = new_config;
}

ptr<resp_msg> raft_server::handle_extended_msg(req_msg& req) {
    switch (req.get_type())
    {
    case msg_type::add_server_request:
        return handle_add_srv_req(req);
    case msg_type::remove_server_request:
        return handle_rm_srv_req(req);
    case msg_type::sync_log_request:
        return handle_log_sync_req(req);
    case msg_type::join_cluster_request:
        return handle_join_cluster_req(req);
    case msg_type::leave_cluster_request:
        return handle_leave_cluster_req(req);
    case msg_type::install_snapshot_request:
        return handle_install_snapshot_req(req);
    default:
        l_.err(sstrfmt("receive an unknown request %s, for safety, step down.").fmt(__msg_type_str[req.get_type()]));
        ctx_->state_mgr_.system_exit(-1);
        ::exit(-1);
        break;
    }

    return ptr<resp_msg>();
}

ptr<resp_msg> raft_server::handle_install_snapshot_req(req_msg& req) {
    if (req.get_term() == state_->get_term() && !catching_up_) {
        if (role_ == srv_role::candidate) {
            become_follower();
        }
        else if (role_ == srv_role::leader) {
            l_.err(lstrfmt("Receive InstallSnapshotRequest from another leader(%d) with same term, there must be a bug, server exits").fmt(req.get_src()));
            ctx_->state_mgr_.system_exit(-1);
            ::exit(-1);
            return ptr<resp_msg>();
        }
        else {
            restart_election_timer();
        }
    }

    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::install_snapshot_response, id_, req.get_src()));
    if (!catching_up_ && req.get_term() < state_->get_term()) {
        l_.info("received an install snapshot request which has lower term than this server, decline the request");
        return resp;
    }

    std::vector<ptr<log_entry>>& entries(req.log_entries());
    if (entries.size() != 1 || entries[0]->get_val_type() != log_val_type::snp_sync_req) {
        l_.warn("Receive an invalid InstallSnapshotRequest due to bad log entries or bad log entry value");
        return resp;
    }

    ptr<snapshot_sync_req> sync_req(snapshot_sync_req::deserialize(entries[0]->get_buf()));
    if (sync_req->get_snapshot().get_last_log_idx() <= state_->get_commit_idx()) {
        l_.warn(sstrfmt("received a snapshot (%llu) that is older than current log store").fmt(sync_req->get_snapshot().get_last_log_idx()));
        return resp;
    }

    if (handle_snapshot_sync_req(*sync_req)) {
        resp->accept(sync_req->get_offset() + sync_req->get_data().size());
    }
    
    return resp;
}

bool raft_server::handle_snapshot_sync_req(snapshot_sync_req& req) {
    try {
        state_machine_.save_snapshot_data(req.get_snapshot(), req.get_offset(), req.get_data());
        if (req.is_done()) {
            // Only follower will run this piece of code, but let's check it again
            if (role_ != srv_role::follower) {
                l_.err("bad server role for applying a snapshot, exit for debugging");
                ctx_->state_mgr_.system_exit(-1);
                ::exit(-1);
            }

            l_.debug("sucessfully receive a snapshot from leader");
            if (log_store_->compact(req.get_snapshot().get_last_log_idx())) {
                // The state machine will not be able to commit anything before the snapshot is applied, so make this synchronously
                // with election timer stopped as usually applying a snapshot may take a very long time
                stop_election_timer();
                l_.info("successfully compact the log store, will now ask the statemachine to apply the snapshot");
                if (!state_machine_.apply_snapshot(req.get_snapshot())) {
                    l_.info("failed to apply the snapshot after log compacted, to ensure the safety, will shutdown the system");
                    ctx_->state_mgr_.system_exit(-1);
                    ::exit(-1);
                    return false;
                }

                reconfigure(req.get_snapshot().get_last_config());
                ctx_->state_mgr_.save_config(*config_);
                state_->set_commit_idx(req.get_snapshot().get_last_log_idx());
                quick_commit_idx_ = req.get_snapshot().get_last_log_idx();
                ctx_->state_mgr_.save_state(*state_);
                restart_election_timer();
                l_.info("snapshot is successfully applied");
            }
            else {
                l_.err("failed to compact the log store after a snapshot is received, will ask the leader to retry");
                return false;
            }
        }
    }
    catch (...) {
        l_.err("failed to handle snapshot installation due to system errors");
        ctx_->state_mgr_.system_exit(-1);
        ::exit(-1);
        return false;
    }

    return true;
}

void raft_server::handle_ext_resp(ptr<resp_msg>& resp, ptr<rpc_exception>& err) {
    recur_lock(lock_);
    if (err) {
        handle_ext_resp_err(*err);
        return;
    }

    l_.debug(
        lstrfmt("Receive an extended %s message from peer %d with Result=%d, Term=%llu, NextIndex=%llu")
        .fmt(
            __msg_type_str[resp->get_type()],
            resp->get_src(),
            resp->get_accepted() ? 1 : 0,
            resp->get_term(),
            resp->get_next_idx()));

    switch (resp->get_type())
    {
    case msg_type::sync_log_response:
        if (srv_to_join_) {
            // we are reusing heartbeat interval value to indicate when to stop retry
            srv_to_join_->resume_hb_speed();
            srv_to_join_->set_next_log_idx(resp->get_next_idx());
            srv_to_join_->set_matched_idx(resp->get_next_idx() - 1);
            sync_log_to_new_srv(resp->get_next_idx()); 
        }
        break;
    case msg_type::join_cluster_response:
        if (srv_to_join_) {
            if (resp->get_accepted()) {
                l_.debug("new server confirms it will join, start syncing logs to it");
                sync_log_to_new_srv(1);
            }
            else {
                l_.debug("new server cannot accept the invitation, give up");
            }
        }
        else {
            l_.debug("no server to join, drop the message");
        }
        break;
    case msg_type::leave_cluster_response:
        if (!resp->get_accepted()) {
            l_.debug("peer doesn't accept to stepping down, stop proceeding");
            return;
        }

        l_.debug("peer accepted to stepping down, removing this server from cluster");
        rm_srv_from_cluster(resp->get_src());
        break;
    case msg_type::install_snapshot_response:
        {
            if (!srv_to_join_) {
                l_.info("no server to join, the response must be very old.");
                return;
            }

            if (!resp->get_accepted()) {
                l_.info("peer doesn't accept the snapshot installation request");
                return;
            }

            ptr<snapshot_sync_ctx> sync_ctx = srv_to_join_->get_snapshot_sync_ctx();
            if (sync_ctx == nilptr) {
                l_.err("Bug! SnapshotSyncContext must not be null");
                ctx_->state_mgr_.system_exit(-1);
                ::exit(-1);
                return;
            }

            if (resp->get_next_idx() >= sync_ctx->get_snapshot()->size()) {
                // snapshot is done
                ptr<snapshot> nil_snap;
                l_.debug("snapshot has been copied and applied to new server, continue to sync logs after snapshot");
                srv_to_join_->set_snapshot_in_sync(nil_snap);
                srv_to_join_->set_next_log_idx(sync_ctx->get_snapshot()->get_last_log_idx() + 1);
                srv_to_join_->set_matched_idx(sync_ctx->get_snapshot()->get_last_log_idx());
            }
            else {
                sync_ctx->set_offset(resp->get_next_idx());
                l_.debug(sstrfmt("continue to send snapshot to new server at offset %llu").fmt(resp->get_next_idx()));
            }

            sync_log_to_new_srv(srv_to_join_->get_next_log_idx());
        }
        break;
    default:
        l_.err(lstrfmt("received an unexpected response message type %s, for safety, stepping down").fmt(__msg_type_str[resp->get_type()]));
        ctx_->state_mgr_.system_exit(-1);
        ::exit(-1);
        break;
    }
}

void raft_server::handle_ext_resp_err(rpc_exception& err) {
    l_.debug(lstrfmt("receive an rpc error response from peer server, %s").fmt(err.what()));
    ptr<req_msg> req = err.req();
    if (req->get_type() == msg_type::sync_log_request ||
        req->get_type() == msg_type::join_cluster_request ||
        req->get_type() == msg_type::leave_cluster_request) {
        ptr<peer> p;
        if (req->get_type() == msg_type::leave_cluster_request) {
            peer_itor pit = peers_.find(req->get_dst());
            if (pit != peers_.end()) {
                p = pit->second;
            }
        }
        else {
            p = srv_to_join_;
        }

        if (p != nilptr) {
            if (p->get_current_hb_interval() >= ctx_->params_->max_hb_interval()) {
                if (req->get_type() == msg_type::leave_cluster_request) {
                    l_.info(lstrfmt("rpc failed again for the removing server (%d), will remove this server directly").fmt(p->get_id()));

                    /**
                    * In case of there are only two servers in the cluster, it safe to remove the server directly from peers
                    * as at most one config change could happen at a time
                    *  prove:
                    *      assume there could be two config changes at a time
                    *      this means there must be a leader after previous leader offline, which is impossible
                    *      (no leader could be elected after one server goes offline in case of only two servers in a cluster)
                    * so the bug https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
                    * does not apply to cluster which only has two members
                    */
                    if (peers_.size() == 1) {
                        peer_itor pit = peers_.find(p->get_id());
                        if (pit != peers_.end()) {
                            pit->second->enable_hb(false);
                            peers_.erase(pit);
                            l_.info(sstrfmt("server %d is removed from cluster").fmt(p->get_id()));
                        }
                        else {
                            l_.info(sstrfmt("peer %d cannot be found, no action for removing").fmt(p->get_id()));
                        }
                    }

                    rm_srv_from_cluster(p->get_id());
                }
                else {
                    l_.info(lstrfmt("rpc failed again for the new coming server (%d), will stop retry for this server").fmt(p->get_id()));
                    config_changing_ = false;
                    srv_to_join_.reset();
                }
            }
            else {
                // reuse the heartbeat interval value to indicate when to stop retrying, as rpc backoff is the same
                l_.debug("retry the request");
                p->slow_down_hb();
                timer_task<void>::executor exec = (timer_task<void>::executor)std::bind(&raft_server::on_retryable_req_err, this, p, req);
                ptr<delayed_task> task(cs_new<timer_task<void>>(exec));
                scheduler_.schedule(task, p->get_current_hb_interval());
            }
        }
    }
}

void raft_server::on_retryable_req_err(ptr<peer>& p, ptr<req_msg>& req) {
    l_.debug(sstrfmt("retry the request %s for %d").fmt(__msg_type_str[req->get_type()], p->get_id()));
    p->send_req(req, ex_resp_handler_);
}

ptr<resp_msg> raft_server::handle_rm_srv_req(req_msg& req) {
    std::vector<ptr<log_entry>>& entries(req.log_entries());
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::remove_server_response, id_, leader_));
    if (entries.size() != 1 || entries[0]->get_buf().size() != sz_int) {
        l_.info("bad remove server request as we are expecting one log entry with value type of int");
        return resp;
    }

    if (role_ != srv_role::leader) {
        l_.info("this is not a leader, cannot handle RemoveServerRequest");
        return resp;
    }

    if (config_changing_) {
        // the previous config has not committed yet
        l_.info("previous config has not committed yet");
        return resp;
    }

    int32 srv_id = entries[0]->get_buf().get_int();
    if (srv_id == id_) {
        l_.info("cannot request to remove leader");
        return resp;
    }

    peer_itor pit = peers_.find(srv_id);
    if (pit == peers_.end()) {
        l_.info(sstrfmt("server %d does not exist").fmt(srv_id));
        return resp;
    }

    ptr<peer> p = pit->second;
    ptr<req_msg> leave_req(cs_new<req_msg>(state_->get_term(), msg_type::leave_cluster_request, id_, srv_id, 0, log_store_->next_slot() - 1, quick_commit_idx_));
    p->send_req(leave_req, ex_resp_handler_);
    resp->accept(log_store_->next_slot());
    return resp;
}

ptr<resp_msg> raft_server::handle_add_srv_req(req_msg& req) {
    std::vector<ptr<log_entry>>& entries(req.log_entries());
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::add_server_response, id_, leader_));
    if (entries.size() != 1 || entries[0]->get_val_type() != log_val_type::cluster_server) {
        l_.debug("bad add server request as we are expecting one log entry with value type of ClusterServer");
        return resp;
    }

    if (role_ != srv_role::leader) {
        l_.info("this is not a leader, cannot handle AddServerRequest");
        return resp;
    }

    ptr<srv_config> srv_conf(srv_config::deserialize(entries[0]->get_buf()));
    if (peers_.find(srv_conf->get_id()) != peers_.end() || id_ == srv_conf->get_id()) {
        l_.warn(lstrfmt("the server to be added has a duplicated id with existing server %d").fmt(srv_conf->get_id()));
        return resp;
    }

    if (config_changing_) {
        // the previous config has not committed yet
        l_.info("previous config has not committed yet");
        return resp;
    }

    conf_to_add_ = std::move(srv_conf);
    timer_task<peer&>::executor exec = (timer_task<peer&>::executor)std::bind(&raft_server::handle_hb_timeout, this, std::placeholders::_1);
    srv_to_join_ = cs_new<peer, srv_config&, context&, timer_task<peer&>::executor&>(*conf_to_add_, *ctx_, exec);
    invite_srv_to_join_cluster();
    resp->accept(log_store_->next_slot());
    return resp;
}

ptr<resp_msg> raft_server::handle_log_sync_req(req_msg& req) {
    std::vector<ptr<log_entry>>& entries = req.log_entries();
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::sync_log_response, id_, req.get_src()));
    if (entries.size() != 1 || entries[0]->get_val_type() != log_val_type::log_pack) {
        l_.info("receive an invalid LogSyncRequest as the log entry value doesn't meet the requirements");
        return resp;
    }

    if (!catching_up_) {
        l_.info("This server is ready for cluster, ignore the request");
        return resp;
    }

    log_store_->apply_pack(req.get_last_log_idx() + 1, entries[0]->get_buf());
    commit(log_store_->next_slot() - 1);
    resp->accept(log_store_->next_slot());
    return resp;
}

void raft_server::sync_log_to_new_srv(ulong start_idx) {
    // only sync committed logs
    int32 gap = (int32)(quick_commit_idx_ - start_idx);
    if (gap < ctx_->params_->log_sync_stop_gap_) {
        l_.info(lstrfmt("LogSync is done for server %d with log gap %d, now put the server into cluster").fmt(srv_to_join_->get_id(), gap));
        ptr<cluster_config> new_conf = cs_new<cluster_config>(log_store_->next_slot(), config_->get_log_idx());
        new_conf->get_servers().insert(new_conf->get_servers().end(), config_->get_servers().begin(), config_->get_servers().end());
        new_conf->get_servers().push_back(conf_to_add_);
        ptr<buffer> new_conf_buf(new_conf->serialize());
        ptr<log_entry> entry(cs_new<log_entry>(state_->get_term(), new_conf_buf, log_val_type::conf));
        log_store_->append(entry);
        config_changing_ = true;
        request_append_entries();
        return;
    }

    ptr<req_msg> req;
    if (start_idx > 0 && start_idx < log_store_->start_index()) {
        req = create_sync_snapshot_req(*srv_to_join_, start_idx, state_->get_term(), quick_commit_idx_);
    }
    else {
        int32 size_to_sync = std::min(gap, ctx_->params_->log_sync_batch_size_);
        ptr<buffer> log_pack = log_store_->pack(start_idx, size_to_sync);
        req = cs_new<req_msg>(state_->get_term(), msg_type::sync_log_request, id_, srv_to_join_->get_id(), 0L, start_idx - 1, quick_commit_idx_);
        req->log_entries().push_back(cs_new<log_entry>(state_->get_term(), log_pack, log_val_type::log_pack));
    }

    srv_to_join_->send_req(req, ex_resp_handler_);
}

void raft_server::invite_srv_to_join_cluster() {
    ptr<req_msg> req(cs_new<req_msg>(state_->get_term(), msg_type::join_cluster_request, id_, srv_to_join_->get_id(), 0L, log_store_->next_slot() - 1, quick_commit_idx_));
    req->log_entries().push_back(cs_new<log_entry>(state_->get_term(), config_->serialize(), log_val_type::conf));
    srv_to_join_->send_req(req, ex_resp_handler_);
}

ptr<resp_msg> raft_server::handle_join_cluster_req(req_msg& req) {
    std::vector<ptr<log_entry>>& entries = req.log_entries();
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::join_cluster_response, id_, req.get_src()));
    if (entries.size() != 1 || entries[0]->get_val_type() != log_val_type::conf) {
        l_.info("receive an invalid JoinClusterRequest as the log entry value doesn't meet the requirements");
        return resp;
    }

    if (catching_up_) {
        l_.info("this server is already in log syncing mode");
        return resp;
    }

    catching_up_ = true;
    role_ = srv_role::follower;
    leader_ = req.get_src();
    state_->set_commit_idx(0);
    quick_commit_idx_ = 0;
    state_->set_voted_for(-1);
    state_->set_term(req.get_term());
    ctx_->state_mgr_.save_state(*state_);
    reconfigure(cluster_config::deserialize(entries[0]->get_buf()));
    resp->accept(log_store_->next_slot());
    return resp;
}

ptr<resp_msg> raft_server::handle_leave_cluster_req(req_msg& req) {
    ptr<resp_msg> resp(cs_new<resp_msg>(state_->get_term(), msg_type::leave_cluster_response, id_, req.get_src()));
    if (!config_changing_) {
        steps_to_down_ = 2;
        resp->accept(log_store_->next_slot());
    }

    return resp;
}

void raft_server::rm_srv_from_cluster(int32 srv_id) {
    ptr<cluster_config> new_conf = cs_new<cluster_config>(log_store_->next_slot(), config_->get_log_idx());
    for (cluster_config::const_srv_itor it = config_->get_servers().begin(); it != config_->get_servers().end(); ++it) {
        if ((*it)->get_id() != srv_id) {
            new_conf->get_servers().push_back(*it);
        }
    }

    l_.info(lstrfmt("removed a server from configuration and save the configuration to log store at %llu").fmt(new_conf->get_log_idx()));
    config_changing_ = true;
    ptr<buffer> new_conf_buf(new_conf->serialize());
    ptr<log_entry> entry(cs_new<log_entry>(state_->get_term(), new_conf_buf, log_val_type::conf));
    log_store_->append(entry);
    request_append_entries();
}

int32 raft_server::get_snapshot_sync_block_size() const {
    int32 block_size = ctx_->params_->snapshot_block_size_;
    return block_size == 0 ? default_snapshot_sync_block_size : block_size;
}

ptr<req_msg> raft_server::create_sync_snapshot_req(peer& p, ulong last_log_idx, ulong term, ulong commit_idx) {
    std::lock_guard<std::mutex> guard(p.get_lock());
    ptr<snapshot_sync_ctx> sync_ctx = p.get_snapshot_sync_ctx();
    ptr<snapshot> snp;
    if (sync_ctx != nilptr) {
        snp = sync_ctx->get_snapshot();
    }

    ptr<snapshot> last_snp(state_machine_.last_snapshot());
    if (!snp || (last_snp && last_snp->get_last_log_idx() > snp->get_last_log_idx())) {
        snp = last_snp;
        if (snp == nilptr || last_log_idx > snp->get_last_log_idx()) {
            l_.err(
                lstrfmt("system is running into fatal errors, failed to find a snapshot for peer %d(snapshot null: %d, snapshot doesn't contais lastLogIndex: %d")
                .fmt(p.get_id(), snp == nilptr ? 1 : 0, last_log_idx > snp->get_last_log_idx() ? 1 : 0));
            ctx_->state_mgr_.system_exit(-1);
            ::exit(-1);
            return ptr<req_msg>();
        }

        if (snp->size() < 1L) {
            l_.err("invalid snapshot, this usually means a bug from state machine implementation, stop the system to prevent further errors");
            ctx_->state_mgr_.system_exit(-1);
            ::exit(-1);
            return ptr<req_msg>();
        }

        l_.info(sstrfmt("trying to sync snapshot with last index %llu to peer %d").fmt(snp->get_last_log_idx(), p.get_id()));
        p.set_snapshot_in_sync(snp);
    }

    ulong offset = p.get_snapshot_sync_ctx()->get_offset();
    int32 sz_left = (int32)(snp->size() - offset);
    int32 blk_sz = get_snapshot_sync_block_size();
    ptr<buffer> data = buffer::alloc((size_t)(std::min(blk_sz, sz_left)));
    int32 sz_rd = state_machine_.read_snapshot_data(*snp, offset, *data);
    if ((size_t)sz_rd < data->size()) {
        l_.err(lstrfmt("only %d bytes could be read from snapshot while %d bytes are expected, must be something wrong, exit.").fmt(sz_rd, data->size()));
        ctx_->state_mgr_.system_exit(-1);
        ::exit(-1);
        return ptr<req_msg>();
    }

    std::unique_ptr<snapshot_sync_req> sync_req(new snapshot_sync_req(snp, offset, data, (offset + (ulong)data->size()) >= snp->size()));
    ptr<req_msg> req(cs_new<req_msg>(term, msg_type::install_snapshot_request, id_, p.get_id(), snp->get_last_log_term(), snp->get_last_log_idx(), commit_idx));
    req->log_entries().push_back(cs_new<log_entry>(term, sync_req->serialize(), log_val_type::snp_sync_req));
    return req;
}

ulong raft_server::term_for_log(ulong log_idx) {
    if (log_idx == 0) {
        return 0L;
    }

    if (log_idx >= log_store_->start_index()) {
        return log_store_->term_at(log_idx);
    }

    ptr<snapshot> last_snapshot(state_machine_.last_snapshot());
    if (!last_snapshot || log_idx != last_snapshot->get_last_log_idx()) {
        l_.err(sstrfmt("bad log_idx %llu for retrieving the term value, kill the system to protect the system").fmt(log_idx));
        ctx_->state_mgr_.system_exit(-1);
        ::exit(-1);
    }

    return last_snapshot->get_last_log_term();
}

void raft_server::commit_in_bg() {
    while (true) {
        try {
            ulong current_commit_idx = state_->get_commit_idx();
            while (quick_commit_idx_ <= current_commit_idx
                || current_commit_idx >= log_store_->next_slot() - 1) {
                std::unique_lock<std::mutex> lock(commit_lock_);
                commit_cv_.wait(lock);
                if (stopping_) {
                    lock.unlock();
                    lock.release();
                    {
                        auto_lock(stopping_lock_);
                        ready_to_stop_cv_.notify_all();
                    }

                    return;
                }

                current_commit_idx = state_->get_commit_idx();
            }

            while (current_commit_idx < quick_commit_idx_ && current_commit_idx < log_store_->next_slot() - 1) {
                current_commit_idx += 1;
                ptr<log_entry> log_entry(log_store_->entry_at(current_commit_idx));
                if (log_entry->get_val_type() == log_val_type::app_log) {
                    state_machine_.commit(current_commit_idx, log_entry->get_buf());
                } else if (log_entry->get_val_type() == log_val_type::conf) {
                    recur_lock(lock_);
                    log_entry->get_buf().pos(0);
                    ptr<cluster_config> new_conf = cluster_config::deserialize(log_entry->get_buf());
                    l_.info(sstrfmt("config at index %llu is committed").fmt(new_conf->get_log_idx()));
                    ctx_->state_mgr_.save_config(*new_conf);
                    config_changing_ = false;
                    if (config_->get_log_idx() < new_conf->get_log_idx()) {
                        reconfigure(new_conf);
                    }

                    if (catching_up_ && new_conf->get_server(id_) != nilptr) {
                        l_.info("this server is committed as one of cluster members");
                        catching_up_ = false;
                    }
                }

                state_->set_commit_idx(current_commit_idx);
                snapshot_and_compact(current_commit_idx);
            }

            ctx_->state_mgr_.save_state(*state_);
        }catch(std::exception& err){
            l_.err(lstrfmt("background committing thread encounter err %s, exiting to protect the system").fmt(err.what()));
            ctx_->state_mgr_.system_exit(-1);
            ::exit(-1);
        }
    }
}


ptr<async_result<bool>> raft_server::add_srv(const srv_config& srv) {
    ptr<buffer> buf(srv.serialize());
    ptr<log_entry> log(cs_new<log_entry>(0, buf, log_val_type::cluster_server));
    ptr<req_msg> req(cs_new<req_msg>((ulong)0, msg_type::add_server_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
    req->log_entries().push_back(log);
    return send_msg_to_leader(req);
}

ptr<async_result<bool>> raft_server::append_entries(const std::vector<ptr<buffer>>& logs) {
    if (logs.size() == 0) {
        bool result(false);
        return cs_new<async_result<bool>>(result);
    }

    ptr<req_msg> req(cs_new<req_msg>((ulong)0, msg_type::client_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
    for (std::vector<ptr<buffer>>::const_iterator it = logs.begin(); it != logs.end(); ++it) {
        ptr<log_entry> log(cs_new<log_entry>(0, *it, log_val_type::app_log));
        req->log_entries().push_back(log);
    }

    return send_msg_to_leader(req);
}

ptr<async_result<bool>> raft_server::remove_srv(const int srv_id) {
    ptr<buffer> buf(buffer::alloc(sz_int));
    buf->put(srv_id);
    buf->pos(0);
    ptr<log_entry> log(cs_new<log_entry>(0, buf, log_val_type::cluster_server));
    ptr<req_msg> req(cs_new<req_msg>((ulong)0, msg_type::remove_server_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
    req->log_entries().push_back(log);
    return send_msg_to_leader(req);
}

ptr<async_result<bool>> raft_server::send_msg_to_leader(ptr<req_msg>& req) {
    typedef std::unordered_map<int32, ptr<rpc_client>>::const_iterator rpc_client_itor;
    int32 leader_id = leader_;
    ptr<cluster_config> cluster = config_;
    bool result(false);
    if (leader_id == -1) {
        return cs_new<async_result<bool>>(result);
    }

    if (leader_id == id_) {
        ptr<resp_msg> resp = process_req(*req);
        result = resp->get_accepted();
        return cs_new<async_result<bool>>(result);
    }

    ptr<rpc_client> rpc_cli;
    {
        auto_lock(rpc_clients_lock_);
        rpc_client_itor itor = rpc_clients_.find(leader_id);
        if (itor == rpc_clients_.end()) {
            ptr<srv_config> srv_conf = config_->get_server(leader_id);
            if (!srv_conf) {
                return cs_new<async_result<bool>>(result);
            }

            rpc_cli = ctx_->rpc_cli_factory_.create_client(srv_conf->get_endpoint());
            rpc_clients_.insert(std::make_pair(leader_id, rpc_cli));
        }else{
            rpc_cli = itor->second;
        }
    }

    if (!rpc_cli) {
        return cs_new<async_result<bool>>(result);
    }

    ptr<async_result<bool>> presult(cs_new<async_result<bool>>());
    rpc_handler handler = [presult](ptr<resp_msg>& resp, ptr<rpc_exception>& err) -> void {
        bool rpc_success(false);
        ptr<std::exception> perr;
        if (err) {
            perr = err;
        }
        else {
            rpc_success = resp && resp->get_accepted();
        }

        presult->set_result(rpc_success, perr);
    };
    rpc_cli->send(req, handler);
    return presult;
}