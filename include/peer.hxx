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

#ifndef _PEER_HXX_
#define _PEER_HXX_

namespace cornerstone {
    class peer {
    public:
        peer(ptr<srv_config>& config, const context& ctx, timer_task<peer&>::executor& hb_exec)
            : config_(config),
            scheduler_(ctx.scheduler_),
            rpc_(ctx.rpc_cli_factory_->create_client(config->get_endpoint())),
            current_hb_interval_(ctx.params_->heart_beat_interval_),
            hb_interval_(ctx.params_->heart_beat_interval_),
            rpc_backoff_(ctx.params_->rpc_failure_backoff_),
            max_hb_interval_(ctx.params_->max_hb_interval()),
            next_log_idx_(0),
            matched_idx_(0),
            last_resp_(),
            busy_flag_(false),
            pending_commit_flag_(false),
            hb_enabled_(false),
            hb_task_(cs_new<timer_task<peer&>, timer_task<peer&>::executor&, peer&>(hb_exec, *this)),
            snp_sync_ctx_(),
            lock_(){
        }

    __nocopy__(peer)
    
    public:
        int32 get_id() const {
            return config_->get_id();
        }

        const srv_config& get_config() {
            return *config_;
        }

        ptr<delayed_task>& get_hb_task() {
            return hb_task_;
        }

        std::mutex& get_lock() {
            return lock_;
        }

        int32 get_current_hb_interval() const {
            return current_hb_interval_;
        }

        bool make_busy() {
            bool f = false;
            return busy_flag_.compare_exchange_strong(f, true);
        }

        void set_free() {
            busy_flag_.store(false);
        }

        bool is_hb_enabled() const {
            return hb_enabled_;
        }

        void enable_hb(bool enable) {
            hb_enabled_ = enable;
            if (!enable) {
                scheduler_->cancel(hb_task_);
            }
        }

        ulong get_next_log_idx() const {
            return next_log_idx_;
        }

        void set_next_log_idx(ulong idx) {
            next_log_idx_ = idx;
        }

        ulong get_matched_idx() const {
            return matched_idx_;
        }

        void set_matched_idx(ulong idx) {
            matched_idx_ = idx;
        }

        const time_point& get_last_resp() const {
            return last_resp_;
        }

        template<typename T>
        void set_last_resp(T&& value) {
            last_resp_ = std::forward<T>(value);
        }

        void set_pending_commit() {
            pending_commit_flag_.store(true);
        }

        bool clear_pending_commit() {
            bool t = true;
            return pending_commit_flag_.compare_exchange_strong(t, false);
        }

        void set_snapshot_in_sync(const ptr<snapshot>& s) {
            if (s == nilptr) {
                snp_sync_ctx_.reset();
            }
            else {
                snp_sync_ctx_ = cs_new<snapshot_sync_ctx>(s);
            }
        }

        ptr<snapshot_sync_ctx> get_snapshot_sync_ctx() const {
            return snp_sync_ctx_;
        }

        void slow_down_hb() {
            current_hb_interval_ = std::min(max_hb_interval_, current_hb_interval_ + rpc_backoff_);
        }

        void resume_hb_speed() {
            current_hb_interval_ = hb_interval_;
        }

        void send_req(ptr<req_msg>& req, rpc_handler& handler);
    private:
        void handle_rpc_result(ptr<req_msg>& req, ptr<rpc_result>& pending_result, ptr<resp_msg>& resp, const ptr<rpc_exception>& err);
    private:
        ptr<srv_config> config_;
        ptr<delayed_task_scheduler> scheduler_;
        ptr<rpc_client> rpc_;
        int32 current_hb_interval_;
        int32 hb_interval_;
        int32 rpc_backoff_;
        int32 max_hb_interval_;
        ulong next_log_idx_;
        ulong matched_idx_;
        time_point last_resp_;
        std::atomic_bool busy_flag_;
        std::atomic_bool pending_commit_flag_;
        bool hb_enabled_;
        ptr<delayed_task> hb_task_;
        ptr<snapshot_sync_ctx> snp_sync_ctx_;
        std::mutex lock_;
    };
}

#endif //_PEER_HXX_
