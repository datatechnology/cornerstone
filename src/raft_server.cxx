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

#include "raft_server.hxx"
#include <random>
#include <thread>
#include "strfmt.hxx"

using namespace cornerstone;

const int raft_server::default_snapshot_sync_block_size = 4 * 1024;

// for tracing and debugging
const char* __msg_type_str[] = {
    "unknown",
    "vote_request",
    "vote_response",
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
    "install_snapshot_response",
    "prevote_request",
    "prevote_response"};

raft_server::raft_server(context* ctx)
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
      election_exec_([this]() { this->handle_election_timeout(); }),
      election_task_(),
      peers_(),
      peers_lock_(),
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
      resp_handler_([this](ptr<resp_msg>& resp, const ptr<rpc_exception>& e) { this->handle_peer_resp(resp, e); }),
      ex_resp_handler_([this](ptr<resp_msg>& resp, const ptr<rpc_exception>& e) { this->handle_ext_resp(resp, e); }),
      last_snapshot_(ctx->state_machine_->last_snapshot()),
      voted_servers_(),
      prevote_state_()
{
    std::random_device rd;
    std::default_random_engine engine(rd());
    std::uniform_int_distribution<int32> distribution(
        ctx->params_->election_timeout_lower_bound_, ctx->params_->election_timeout_upper_bound_);
    rand_timeout_ = [distribution, engine]() mutable -> int32_t { return distribution(engine); };

    if (!state_)
    {
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
     * Case #1, There is no configuration change since S0, then it's obvious that Vote(A) < Majority(S0), see the core
     * Algorithm Case #2, There are one or more configuration changes since S0, then at the time of first configuration
     * change was committed, there are at least Majority(S0 - 1) servers committed the configuration change Majority(S0
     * - 1) + Majority(S0) > S0 => Vote(A) < Majority(S0)
     * -|
     */
    for (ulong i = std::max(sm_commit_index_ + 1, log_store_->start_index()); i < log_store_->next_slot(); ++i)
    {
        ptr<log_entry> entry(log_store_->entry_at(i));
        if (entry->get_val_type() == log_val_type::conf)
        {
            l_->info(sstrfmt("detect a configuration change that is not committed yet at index %llu").fmt(i));
            config_changing_ = true;
            break;
        }
    }

    std::list<ptr<srv_config>>& srvs(config_->get_servers());
    for (cluster_config::srv_itor it = srvs.begin(); it != srvs.end(); ++it)
    {
        if ((*it)->get_id() != id_)
        {
            write_lock(peers_lock_);
            timer_task<peer&>::executor exec = [this](peer& p) { this->handle_hb_timeout(p); };
            peers_.insert(std::make_pair(
                (*it)->get_id(),
                cs_new<peer>(*it, *ctx_, exec)));
        }
    }

    std::thread commiting_thread = std::thread([this]() { this->commit_in_bg(); });
    commiting_thread.detach();
    restart_election_timer();
    l_->debug(strfmt<30>("server %d started").fmt(id_));
}

raft_server::~raft_server()
{
    recur_lock(lock_);
    stopping_ = true;
    std::unique_lock<std::mutex> commit_lock(commit_lock_);
    commit_cv_.notify_all();
    std::unique_lock<std::mutex> lock(stopping_lock_);
    commit_lock.unlock();
    commit_lock.release();
    ready_to_stop_cv_.wait(lock);
    if (election_task_)
    {
        scheduler_->cancel(election_task_);
    }

    {
        read_lock(peers_lock_);
        for (peer_itor it = peers_.begin(); it != peers_.end(); ++it)
        {
            if (it->second->get_hb_task())
            {
                scheduler_->cancel(it->second->get_hb_task());
            }
        }
    }
}

void raft_server::handle_election_timeout()
{
    recur_lock(lock_);
    if (steps_to_down_ > 0)
    {
        if (--steps_to_down_ == 0)
        {
            l_->info("no hearing further news from leader, remove this server from cluster and step down");
            for (std::list<ptr<srv_config>>::iterator it = config_->get_servers().begin();
                 it != config_->get_servers().end();
                 ++it)
            {
                if ((*it)->get_id() == id_)
                {
                    config_->get_servers().erase(it);
                    ctx_->state_mgr_->save_config(*config_);
                    break;
                }
            }

            ctx_->state_mgr_->system_exit(-1);
            return;
        }

        l_->info(sstrfmt("stepping down (cycles left: %d), skip this election timeout event").fmt(steps_to_down_));
        restart_election_timer();
        return;
    }

    if (catching_up_)
    {
        // this is a new server for the cluster, will not send out vote req until conf that includes this srv is
        // committed
        l_->info("election timeout while joining the cluster, ignore it.");
        restart_election_timer();
        return;
    }

    if (role_ == srv_role::leader)
    {
        l_->err("A leader should never encounter election timeout, illegal application state, stop the application");
        ctx_->state_mgr_->system_exit(-1);
        return;
    }

    if (ctx_->params_->prevote_enabled_ && role_ == srv_role::follower)
    {
        if (prevote_state_ && !prevote_state_->empty())
        {
            l_->debug("Election timeout, but there is already a prevote ongoing, ignore this event");
        }
        else
        {
            l_->debug("Election timeout, start prevoting");
            request_prevote();
        }
    }
    else
    {
        l_->debug("Election timeout, change to Candidate");
        become_candidate();
    }
}

void raft_server::become_candidate()
{
    prevote_state_.reset();
    state_->inc_term();
    state_->set_voted_for(-1);
    role_ = srv_role::candidate;
    votes_granted_ = 0;
    voted_servers_.clear();
    election_completed_ = false;
    ctx_->state_mgr_->save_state(*state_);
    request_vote();

    // restart the election timer if this is not yet a leader
    if (role_ != srv_role::leader)
    {
        restart_election_timer();
    }
}

void raft_server::request_prevote()
{
    l_->info(sstrfmt("prevote started with term %llu").fmt(state_->get_term()));
    bool change_to_candidate(false);
    {
        read_lock(peers_lock_);
        if (peers_.size() == 0)
        {
            change_to_candidate = true;
        }
    }

    if (change_to_candidate)
    {
        l_->info("prevote done, change to candidate and start voting");
        become_candidate();
        return;
    }

    if (!prevote_state_)
    {
        prevote_state_ = std::make_unique<prevote_state>();
    }

    prevote_state_->inc_accepted_votes();
    prevote_state_->add_voted_server(id_);
    {
        read_lock(peers_lock_);
        for (peer_itor it = peers_.begin(); it != peers_.end(); ++it)
        {
            ptr<req_msg> req(cs_new<req_msg>(
                state_->get_term(),
                msg_type::prevote_request,
                id_,
                it->second->get_id(),
                term_for_log(log_store_->next_slot() - 1),
                log_store_->next_slot() - 1,
                quick_commit_idx_));
            l_->debug(sstrfmt("send %s to server %d with term %llu")
                          .fmt(__msg_type_str[req->get_type()], it->second->get_id(), state_->get_term()));
            it->second->send_req(req, ex_resp_handler_);
        }
    }
}

void raft_server::request_vote()
{
    l_->info(sstrfmt("requestVote started with term %llu").fmt(state_->get_term()));
    state_->set_voted_for(id_);
    ctx_->state_mgr_->save_state(*state_);
    votes_granted_ += 1;
    voted_servers_.insert(id_);

    bool change_to_leader(false);
    {
        read_lock(peers_lock_);

        // is this the only server?
        if (votes_granted_ > (int32)(peers_.size() + 1) / 2)
        {
            election_completed_ = true;
            change_to_leader = true;
        }
        else
        {
            for (peer_itor it = peers_.begin(); it != peers_.end(); ++it)
            {
                ptr<req_msg> req(cs_new<req_msg>(
                    state_->get_term(),
                    msg_type::vote_request,
                    id_,
                    it->second->get_id(),
                    term_for_log(log_store_->next_slot() - 1),
                    log_store_->next_slot() - 1,
                    quick_commit_idx_));
                l_->debug(sstrfmt("send %s to server %d with term %llu")
                              .fmt(__msg_type_str[req->get_type()], it->second->get_id(), state_->get_term()));
                it->second->send_req(req, resp_handler_);
            }
        }
    }

    if (change_to_leader)
    {
        become_leader();
    }
}

void raft_server::request_append_entries()
{
    read_lock(peers_lock_);
    if (peers_.size() == 0)
    {
        commit(log_store_->next_slot() - 1);
        return;
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it)
    {
        request_append_entries(*it->second);
    }
}

bool raft_server::request_append_entries(peer& p)
{
    if (p.make_busy())
    {
        ptr<req_msg> msg = create_append_entries_req(p);
        p.send_req(msg, resp_handler_);
        return true;
    }

    l_->debug(sstrfmt("Server %d is busy, skip the request").fmt(p.get_id()));
    return false;
}

void raft_server::handle_hb_timeout(peer& p)
{
    recur_lock(lock_);
    l_->debug(sstrfmt("Heartbeat timeout for %d").fmt(p.get_id()));
    if (role_ == srv_role::leader)
    {
        request_append_entries(p);
        {
            std::lock_guard<std::mutex> guard(p.get_lock());
            if (p.is_hb_enabled())
            {
                // Schedule another heartbeat if heartbeat is still enabled
                scheduler_->schedule(p.get_hb_task(), p.get_current_hb_interval());
            }
            else
            {
                l_->debug(sstrfmt("heartbeat is disabled for peer %d").fmt(p.get_id()));
            }
        }
    }
    else
    {
        l_->info(sstrfmt("Receive a heartbeat event for %d while no longer as a leader").fmt(p.get_id()));
    }
}

void raft_server::restart_election_timer()
{
    // don't start the election timer while this server is still catching up the logs
    if (catching_up_)
    {
        return;
    }

    if (election_task_)
    {
        scheduler_->cancel(election_task_);
    }
    else
    {
        election_task_ = cs_new<timer_task<void>>(election_exec_);
    }

    scheduler_->schedule(election_task_, rand_timeout_());
}

void raft_server::stop_election_timer()
{
    if (!election_task_)
    {
        l_->warn("Election Timer is never started but is requested to stop, protential a bug");
        return;
    }

    scheduler_->cancel(election_task_);
}

void raft_server::become_leader()
{
    stop_election_timer();
    role_ = srv_role::leader;
    leader_ = id_;
    srv_to_join_.reset();
    ptr<snapshot> nil_snp;
    {
        read_lock(peers_lock_);
        for (peer_itor it = peers_.begin(); it != peers_.end(); ++it)
        {
            it->second->set_next_log_idx(log_store_->next_slot());
            it->second->set_snapshot_in_sync(nil_snp);
            it->second->set_free();
            enable_hb_for_peer(*(it->second));
        }
    }

    if (config_->get_log_idx() == 0)
    {
        config_->set_log_idx(log_store_->next_slot());
        bufptr conf_buf = config_->serialize();
        ptr<log_entry> entry(cs_new<log_entry>(state_->get_term(), std::move(conf_buf), log_val_type::conf));
        log_store_->append(entry);
        l_->info("save initial config to log store");
        config_changing_ = true;
    }

    if (ctx_->event_listener_)
    {
        ctx_->event_listener_->on_event(raft_event::become_leader);
    }

    request_append_entries();
}

void raft_server::enable_hb_for_peer(peer& p)
{
    p.enable_hb(true);
    p.resume_hb_speed();
    scheduler_->schedule(p.get_hb_task(), p.get_current_hb_interval());
}

void raft_server::become_follower()
{
    // reset prevote
    prevote_state_.reset();

    // stop hb for all peers
    {
        read_lock(peers_lock_);
        for (peer_itor it = peers_.begin(); it != peers_.end(); ++it)
        {
            it->second->enable_hb(false);
        }
    }

    srv_to_join_.reset();
    role_ = srv_role::follower;
    restart_election_timer();
    if (ctx_->event_listener_)
    {
        ctx_->event_listener_->on_event(raft_event::become_follower);
    }
}

bool raft_server::update_term(ulong term)
{
    if (term > state_->get_term())
    {
        state_->set_term(term);
        state_->set_voted_for(-1);
        election_completed_ = false;
        votes_granted_ = 0;
        voted_servers_.clear();
        ctx_->state_mgr_->save_state(*state_);
        become_follower();
        return true;
    }

    return false;
}

void raft_server::commit(ulong target_idx)
{
    if (target_idx > quick_commit_idx_)
    {
        quick_commit_idx_ = target_idx;

        // if this is a leader notify peers to commit as well
        // for peers that are free, send the request, otherwise, set pending commit flag for that peer
        if (role_ == srv_role::leader)
        {
            read_lock(peers_lock_);
            for (peer_itor it = peers_.begin(); it != peers_.end(); ++it)
            {
                if (!request_append_entries(*(it->second)))
                {
                    it->second->set_pending_commit();
                }
            }
        }
    }

    if (log_store_->next_slot() - 1 > sm_commit_index_ && quick_commit_idx_ > sm_commit_index_)
    {
        commit_cv_.notify_one();
    }
}

void raft_server::snapshot_and_compact(ulong committed_idx)
{
    if (ctx_->params_->snapshot_distance_ == 0 ||
        (committed_idx - log_store_->start_index()) < (ulong)ctx_->params_->snapshot_distance_)
    {
        // snapshot is disabled or the log store is not long enough
        return;
    }

    bool snapshot_in_action = false;
    try
    {
        bool f = false;
        if ((!last_snapshot_ ||
             (committed_idx - last_snapshot_->get_last_log_idx()) >= (ulong)ctx_->params_->snapshot_distance_) &&
            snp_in_progress_.compare_exchange_strong(f, true))
        {
            snapshot_in_action = true;
            l_->info(sstrfmt("creating a snapshot for index %llu").fmt(committed_idx));

            // get the latest configuration info
            ptr<cluster_config> conf(config_);
            while (conf->get_log_idx() > committed_idx && conf->get_prev_log_idx() >= log_store_->start_index())
            {
                ptr<log_entry> conf_log(log_store_->entry_at(conf->get_prev_log_idx()));
                conf = cluster_config::deserialize(conf_log->get_buf());
            }

            if (conf->get_log_idx() > committed_idx && conf->get_prev_log_idx() > 0 &&
                conf->get_prev_log_idx() < log_store_->start_index())
            {
                if (!last_snapshot_)
                {
                    l_->err("No snapshot could be found while no configuration cannot be found in current committed "
                            "logs, this is a system error, exiting");
                    ctx_->state_mgr_->system_exit(-1);
                    return;
                }

                conf = last_snapshot_->get_last_config();
            }
            else if (conf->get_log_idx() > committed_idx && conf->get_prev_log_idx() == 0)
            {
                l_->err("BUG!!! stop the system, there must be a configuration at index one");
                ctx_->state_mgr_->system_exit(-1);
                return;
            }

            ulong log_term_to_compact = log_store_->term_at(committed_idx);
            ptr<snapshot> new_snapshot(cs_new<snapshot>(committed_idx, log_term_to_compact, conf));
            async_result<bool>::handler_type handler =
                [this, new_snapshot](bool result, const ptr<std::exception>& e) mutable
            { this->on_snapshot_completed(new_snapshot, result, e); };
            state_machine_->create_snapshot(*new_snapshot, handler);
            snapshot_in_action = false;
        }
    }
    catch (...)
    {
        l_->err(sstrfmt("failed to compact logs at index %llu due to errors").fmt(committed_idx));
        if (snapshot_in_action)
        {
            bool val = true;
            snp_in_progress_.compare_exchange_strong(val, false);
        }
    }
}

void raft_server::on_snapshot_completed(ptr<snapshot>& s, bool result, const ptr<std::exception>& err)
{
    do
    {
        if (err != nilptr)
        {
            l_->err(lstrfmt("failed to create a snapshot due to %s").fmt(err->what()));
            break;
        }

        if (!result)
        {
            l_->info("the state machine rejects to create the snapshot");
            break;
        }

        {
            recur_lock(lock_);
            l_->debug("snapshot created, compact the log store");

            last_snapshot_ = s;
            if (s->get_last_log_idx() > (ulong)ctx_->params_->reserved_log_items_)
            {
                log_store_->compact(s->get_last_log_idx() - (ulong)ctx_->params_->reserved_log_items_);
            }
        }
    } while (false);
    snp_in_progress_.store(false);
}

ptr<req_msg> raft_server::create_append_entries_req(peer& p)
{
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
        if (p.get_next_log_idx() == 0L)
        {
            p.set_next_log_idx(cur_nxt_idx);
        }

        last_log_idx = p.get_next_log_idx() - 1;
    }

    if (last_log_idx >= cur_nxt_idx)
    {
        l_->err(
            sstrfmt("Peer's lastLogIndex is too large %llu v.s. %llu, server exits").fmt(last_log_idx, cur_nxt_idx));
        ctx_->state_mgr_->system_exit(-1);
        return ptr<req_msg>();
    }

    // for syncing the snapshots, for starting_idx - 1, we can check with last snapshot
    if (last_log_idx > 0 && last_log_idx < starting_idx - 1)
    {
        return create_sync_snapshot_req(p, last_log_idx, term, commit_idx);
    }

    ulong last_log_term = term_for_log(last_log_idx);
    ulong end_idx = std::min(cur_nxt_idx, last_log_idx + 1 + ctx_->params_->max_append_size_);
    ptr<std::vector<ptr<log_entry>>> log_entries(
        (last_log_idx + 1) >= cur_nxt_idx ? ptr<std::vector<ptr<log_entry>>>()
                                          : log_store_->log_entries(last_log_idx + 1, end_idx));
    l_->debug(
        lstrfmt("An AppendEntries Request for %d with LastLogIndex=%llu, LastLogTerm=%llu, EntriesLength=%d, "
                "CommitIndex=%llu and Term=%llu")
            .fmt(p.get_id(), last_log_idx, last_log_term, log_entries ? log_entries->size() : 0, commit_idx, term));
    ptr<req_msg> req(cs_new<req_msg>(
        term, msg_type::append_entries_request, id_, p.get_id(), last_log_term, last_log_idx, commit_idx));
    std::vector<ptr<log_entry>>& v = req->log_entries();
    if (log_entries)
    {
        v.insert(v.end(), log_entries->begin(), log_entries->end());
    }

    return req;
}

void raft_server::reconfigure(const ptr<cluster_config>& new_config)
{
    l_->debug(lstrfmt("system is reconfigured to have %d servers, last config index: %llu, this config index: %llu")
                  .fmt(new_config->get_servers().size(), new_config->get_prev_log_idx(), new_config->get_log_idx()));

    // we only allow one server to be added or removed at a time
    std::vector<int32> srvs_removed;
    std::vector<ptr<srv_config>> srvs_added;
    std::list<ptr<srv_config>>& new_srvs(new_config->get_servers());
    {
        read_lock(peers_lock_);
        for (std::list<ptr<srv_config>>::const_iterator it = new_srvs.begin(); it != new_srvs.end(); ++it)
        {
            peer_itor pit = peers_.find((*it)->get_id());
            if (pit == peers_.end() && id_ != (*it)->get_id())
            {
                srvs_added.push_back(*it);
            }
        }

        for (peer_itor it = peers_.begin(); it != peers_.end(); ++it)
        {
            if (!new_config->get_server(it->first))
            {
                srvs_removed.push_back(it->first);
            }
        }
    }

    if (!new_config->get_server(id_))
    {
        srvs_removed.push_back(id_);
    }

    for (std::vector<ptr<srv_config>>::const_iterator it = srvs_added.begin(); it != srvs_added.end(); ++it)
    {
        ptr<srv_config> srv_added(*it);
        timer_task<peer&>::executor exec = [this](peer& p) { this->handle_hb_timeout(p); };
        ptr<peer> p = cs_new<peer>(srv_added, *ctx_, exec);
        p->set_next_log_idx(log_store_->next_slot());

        // Add to peers
        {
            write_lock(peers_lock_);
            peers_.insert(std::make_pair(srv_added->get_id(), p));
        }

        l_->info(sstrfmt("server %d is added to cluster").fmt(srv_added->get_id()));
        if (role_ == srv_role::leader)
        {
            l_->info(sstrfmt("enable heartbeating for server %d").fmt(srv_added->get_id()));
            enable_hb_for_peer(*p);
            if (srv_to_join_ && srv_to_join_->get_id() == p->get_id())
            {
                p->set_next_log_idx(srv_to_join_->get_next_log_idx());
                srv_to_join_.reset();
            }
        }
    }

    for (std::vector<int32>::const_iterator it = srvs_removed.begin(); it != srvs_removed.end(); ++it)
    {
        int32 srv_removed = *it;
        if (srv_removed == id_ && !catching_up_)
        {
            // this server is removed from cluster
            ctx_->state_mgr_->save_config(*new_config);
            l_->info("server has been removed, step down");
            ctx_->state_mgr_->system_exit(0);
            return;
        }

        {
            write_lock(peers_lock_);
            peer_itor pit = peers_.find(srv_removed);
            if (pit != peers_.end())
            {
                pit->second->enable_hb(false);
                peers_.erase(pit);
                l_->info(sstrfmt("server %d is removed from cluster").fmt(srv_removed));
            }
            else
            {
                l_->info(sstrfmt("peer %d cannot be found, no action for removing").fmt(srv_removed));
            }
        }
    }

    config_ = new_config;
}

int32 raft_server::get_snapshot_sync_block_size() const
{
    int32 block_size = ctx_->params_->snapshot_block_size_;
    return block_size == 0 ? default_snapshot_sync_block_size : block_size;
}

ptr<req_msg> raft_server::create_sync_snapshot_req(peer& p, ulong last_log_idx, ulong term, ulong commit_idx)
{
    std::lock_guard<std::mutex> guard(p.get_lock());
    ptr<snapshot_sync_ctx> sync_ctx = p.get_snapshot_sync_ctx();
    ptr<snapshot> snp;
    if (sync_ctx != nilptr)
    {
        snp = sync_ctx->get_snapshot();
    }

    if (!snp || (last_snapshot_ && last_snapshot_->get_last_log_idx() > snp->get_last_log_idx()))
    {
        snp = last_snapshot_;
        if (snp == nilptr || last_log_idx > snp->get_last_log_idx())
        {
            l_->err(lstrfmt("system is running into fatal errors, failed to find a snapshot for peer %d(snapshot null: "
                            "%d, snapshot doesn't contais lastLogIndex: %d")
                        .fmt(p.get_id(), snp == nilptr ? 1 : 0, last_log_idx > snp->get_last_log_idx() ? 1 : 0));
            ctx_->state_mgr_->system_exit(-1);
            return ptr<req_msg>();
        }

        if (snp->size() < 1L)
        {
            l_->err("invalid snapshot, this usually means a bug from state machine implementation, stop the system to "
                    "prevent further errors");
            ctx_->state_mgr_->system_exit(-1);
            return ptr<req_msg>();
        }

        l_->info(sstrfmt("trying to sync snapshot with last index %llu to peer %d")
                     .fmt(snp->get_last_log_idx(), p.get_id()));
        p.set_snapshot_in_sync(snp);
    }

    ulong offset = p.get_snapshot_sync_ctx()->get_offset();
    int32 sz_left = (int32)(snp->size() - offset);
    int32 blk_sz = get_snapshot_sync_block_size();
    bufptr data = buffer::alloc((size_t)(std::min(blk_sz, sz_left)));
    int32 sz_rd = state_machine_->read_snapshot_data(*snp, offset, *data);
    if ((size_t)sz_rd < data->size())
    {
        l_->err(
            lstrfmt(
                "only %d bytes could be read from snapshot while %d bytes are expected, must be something wrong, exit.")
                .fmt(sz_rd, data->size()));
        ctx_->state_mgr_->system_exit(-1);
        return ptr<req_msg>();
    }

    bool done = (offset + (ulong)data->size()) >= snp->size();
    std::unique_ptr<snapshot_sync_req> sync_req(new snapshot_sync_req(snp, offset, std::move(data), done));
    ptr<req_msg> req(cs_new<req_msg>(
        term,
        msg_type::install_snapshot_request,
        id_,
        p.get_id(),
        snp->get_last_log_term(),
        snp->get_last_log_idx(),
        commit_idx));
    req->log_entries().push_back(cs_new<log_entry>(term, sync_req->serialize(), log_val_type::snp_sync_req));
    return req;
}

ulong raft_server::term_for_log(ulong log_idx)
{
    if (log_idx == 0)
    {
        return 0L;
    }

    if (log_idx >= log_store_->start_index())
    {
        return log_store_->term_at(log_idx);
    }

    ptr<snapshot> last_snapshot(state_machine_->last_snapshot());
    if (!last_snapshot || log_idx != last_snapshot->get_last_log_idx())
    {
        l_->err(sstrfmt("bad log_idx %llu for retrieving the term value, kill the system to protect the system")
                    .fmt(log_idx));
        ctx_->state_mgr_->system_exit(-1);
    }

    return last_snapshot->get_last_log_term();
}

void raft_server::commit_in_bg()
{
    while (true)
    {
        try
        {
            while (quick_commit_idx_ <= sm_commit_index_ || sm_commit_index_ >= log_store_->next_slot() - 1)
            {
                // already catch up all committed logs
                if (ctx_->event_listener_)
                {
                    ctx_->event_listener_->on_event(raft_event::logs_catch_up);
                }

                std::unique_lock<std::mutex> lock(commit_lock_);
                commit_cv_.wait(lock);
                if (stopping_)
                {
                    lock.unlock();
                    lock.release();
                    {
                        auto_lock(stopping_lock_);
                        ready_to_stop_cv_.notify_all();
                    }

                    return;
                }
            }

            while (sm_commit_index_ < quick_commit_idx_ && sm_commit_index_ < log_store_->next_slot() - 1)
            {
                sm_commit_index_ += 1;
                ptr<log_entry> log_entry(log_store_->entry_at(sm_commit_index_));
                if (log_entry->get_val_type() == log_val_type::app_log)
                {
                    state_machine_->commit(sm_commit_index_, log_entry->get_buf(), log_entry->get_cookie());
                }
                else if (log_entry->get_val_type() == log_val_type::conf)
                {
                    recur_lock(lock_);
                    log_entry->get_buf().pos(0);
                    ptr<cluster_config> new_conf = cluster_config::deserialize(log_entry->get_buf());
                    l_->info(sstrfmt("config at index %llu is committed").fmt(new_conf->get_log_idx()));
                    ctx_->state_mgr_->save_config(*new_conf);
                    config_changing_ = false;
                    if (config_->get_log_idx() < new_conf->get_log_idx())
                    {
                        reconfigure(new_conf);
                    }

                    if (catching_up_ && new_conf->get_server(id_) != nilptr)
                    {
                        l_->info("this server is committed as one of cluster members");
                        catching_up_ = false;
                    }
                }

                snapshot_and_compact(sm_commit_index_);
            }
        }
        catch (std::exception& err)
        {
            l_->err(lstrfmt("background committing thread encounter err %s, exiting to protect the system")
                        .fmt(err.what()));
            ctx_->state_mgr_->system_exit(-1);
        }
    }
}

ptr<async_result<bool>> raft_server::add_srv(const srv_config& srv)
{
    bufptr buf(srv.serialize());
    ptr<log_entry> log(cs_new<log_entry>(0, std::move(buf), log_val_type::cluster_server));
    ptr<req_msg> req(cs_new<req_msg>((ulong)0, msg_type::add_server_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
    req->log_entries().push_back(log);
    return send_msg_to_leader(req);
}

ptr<async_result<bool>> raft_server::append_entries(std::vector<bufptr>& logs)
{
    if (logs.size() == 0)
    {
        bool result(false);
        return cs_new<async_result<bool>>(result);
    }

    ptr<req_msg> req(cs_new<req_msg>((ulong)0, msg_type::client_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
    for (auto& item : logs)
    {
        ptr<log_entry> log(cs_new<log_entry>(0, std::move(item), log_val_type::app_log));
        req->log_entries().push_back(log);
    }

    return send_msg_to_leader(req);
}

ptr<async_result<bool>> raft_server::remove_srv(const int srv_id)
{
    bufptr buf(buffer::alloc(sz_int));
    buf->put(srv_id);
    buf->pos(0);
    ptr<log_entry> log(cs_new<log_entry>(0, std::move(buf), log_val_type::cluster_server));
    ptr<req_msg> req(cs_new<req_msg>((ulong)0, msg_type::remove_server_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
    req->log_entries().push_back(log);
    return send_msg_to_leader(req);
}

ptr<async_result<bool>> raft_server::send_msg_to_leader(ptr<req_msg>& req)
{
    typedef std::unordered_map<int32, ptr<rpc_client>>::const_iterator rpc_client_itor;
    int32 leader_id = leader_;
    ptr<cluster_config> cluster = config_;
    bool result(false);
    if (leader_id == -1)
    {
        return cs_new<async_result<bool>>(result);
    }

    if (leader_id == id_)
    {
        ptr<resp_msg> resp = process_req(*req);
        result = resp->get_accepted();
        return cs_new<async_result<bool>>(result);
    }

    ptr<rpc_client> rpc_cli;
    {
        auto_lock(rpc_clients_lock_);
        rpc_client_itor itor = rpc_clients_.find(leader_id);
        if (itor == rpc_clients_.end())
        {
            ptr<srv_config> srv_conf = config_->get_server(leader_id);
            if (!srv_conf)
            {
                return cs_new<async_result<bool>>(result);
            }

            rpc_cli = ctx_->rpc_cli_factory_->create_client(srv_conf->get_endpoint());
            rpc_clients_.insert(std::make_pair(leader_id, rpc_cli));
        }
        else
        {
            rpc_cli = itor->second;
        }
    }

    if (!rpc_cli)
    {
        return cs_new<async_result<bool>>(result);
    }

    ptr<async_result<bool>> presult(cs_new<async_result<bool>>());
    rpc_handler handler = [presult](ptr<resp_msg>& resp, const ptr<rpc_exception>& err) -> void
    {
        bool rpc_success(false);
        ptr<std::exception> perr;
        if (err)
        {
            perr = err;
        }
        else
        {
            rpc_success = resp && resp->get_accepted();
        }

        presult->set_result(rpc_success, perr);
    };
    rpc_cli->send(req, handler);
    return presult;
}

bool raft_server::is_leader() const
{
    static volatile int32 time_elasped_since_quorum_resp(std::numeric_limits<int32>::max());
    read_lock(peers_lock_);
    if (role_ == srv_role::leader && peers_.size() > 0 &&
        time_elasped_since_quorum_resp > ctx_->params_->election_timeout_upper_bound_ * 2)
    {
        std::vector<time_point> peer_resp_times;
        for (auto& peer : peers_)
        {
            peer_resp_times.push_back(peer.second->get_last_resp());
        }

        std::sort(peer_resp_times.begin(), peer_resp_times.end());
        time_elasped_since_quorum_resp =
            static_cast<int32>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   system_clock::now() - peer_resp_times[peers_.size() / 2])
                                   .count());
        if (time_elasped_since_quorum_resp > ctx_->params_->election_timeout_upper_bound_ * 2)
        {
            return false;
        }
    }

    return role_ == srv_role::leader;
}

bool raft_server::replicate_log(bufptr& log, const ptr<void>& cookie, uint cookie_tag)
{
    if (!is_leader())
    {
        return false;
    }

    ptr<log_entry> entry = cs_new<log_entry>(state_->get_term(), std::move(log));
    if (cookie)
    {
        entry->set_cookie(cookie_tag, cookie);
    }

    log_store_->append(entry);
    {
        recur_lock(lock_);
        request_append_entries();
    }

    return false;
}
