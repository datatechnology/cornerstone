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

#include <cassert>
#include <iostream>
#include <queue>
#include "cornerstone.hxx"

using namespace cornerstone;

class in_mem_log_store : public log_store
{
public:
    in_mem_log_store() : log_entries_(), lock_()
    {
        log_entries_.push_back(ptr<log_entry>(cs_new<log_entry>(0L, buffer::alloc(0))));
    }

    __nocopy__(in_mem_log_store)

        public :
        /**
        ** The first available slot of the store, starts with 1
        */
        virtual ulong next_slot() const
    {
        auto_lock(lock_);
        return (ulong)log_entries_.size();
    }

    /**
    ** The start index of the log store, at the very beginning, it must be 1
    ** however, after some compact actions, this could be anything greater or equals to one
    */
    virtual ulong start_index() const
    {
        return 1;
    }

    /**
     * The last log entry in store
     * @return a dummy constant entry with value set to null and term set to zero if no log entry in store
     */
    virtual ptr<log_entry> last_entry() const
    {
        auto_lock(lock_);
        return log_entries_[log_entries_.size() - 1];
    }

    /**
     * Appends a log entry to store
     * @param entry
     */
    virtual ulong append(ptr<log_entry>& entry)
    {
        auto_lock(lock_);
        log_entries_.push_back(entry);
        return (ulong)(log_entries_.size() - 1);
    }

    /**
     * Over writes a log entry at index of {@code index}
     * @param index a value < this->next_slot(), and starts from 1
     * @param entry
     */
    virtual void write_at(ulong index, ptr<log_entry>& entry)
    {
        auto_lock(lock_);
        if (index >= (ulong)log_entries_.size() || index < 1)
        {
            throw std::overflow_error("index out of range");
        }

        log_entries_[(size_t)index] = entry;
        if ((ulong)log_entries_.size() - index > 1)
        {
            log_entries_.erase(log_entries_.begin() + (size_t)index + 1, log_entries_.end());
        }
    }

    /**
     * Get log entries with index between start and end
     * @param start, the start index of log entries
     * @param end, the end index of log entries (exclusive)
     * @return the log entries between [start, end)
     */
    virtual ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end)
    {
        if (start >= end || start >= log_entries_.size())
        {
            return ptr<std::vector<ptr<log_entry>>>();
        }

        ptr<std::vector<ptr<log_entry>>> v = cs_new<std::vector<ptr<log_entry>>>();
        for (size_t i = (size_t)start; i < (size_t)end; ++i)
        {
            v->push_back(entry_at(i));
        }

        return v;
    }

    /**
     * Gets the log entry at the specified index
     * @param index, starts from 1
     * @return the log entry or null if index >= this->next_slot()
     */
    virtual ptr<log_entry> entry_at(ulong index)
    {
        if ((size_t)index >= log_entries_.size())
        {
            return ptr<log_entry>();
        }

        return log_entries_[index];
    }

    virtual ulong term_at(ulong index)
    {
        if ((size_t)index >= log_entries_.size())
        {
            return 0L;
        }

        ptr<log_entry>& p = log_entries_[index];
        return p->get_term();
    }

    /**
     * Pack cnt log items starts from index
     * @param index
     * @param cnt
     * @return log pack
     */
    virtual bufptr pack(ulong, int32)
    {
        return buffer::alloc(0);
    }

    /**
     * Apply the log pack to current log store, starting from index
     * @param index, the log index that start applying the pack, index starts from 1
     * @param pack
     */
    virtual void apply_pack(ulong, buffer&)
    {
        //
    }

    /**
     * Compact the log store by removing all log entries including the log at the last_log_index
     * @param last_log_index
     * @return compact successfully or not
     */
    virtual bool compact(ulong)
    {
        return true;
    }

private:
    std::vector<ptr<log_entry>> log_entries_;
    mutable std::mutex lock_;
};

class in_memory_state_mgr : public state_mgr
{
public:
    in_memory_state_mgr(int32 srv_id) : srv_id_(srv_id)
    {
    }

public:
    virtual ptr<cluster_config> load_config()
    {
        ptr<cluster_config> conf = cs_new<cluster_config>();
        conf->get_servers().push_back(cs_new<srv_config>(1, "port1"));
        conf->get_servers().push_back(cs_new<srv_config>(2, "port2"));
        conf->get_servers().push_back(cs_new<srv_config>(3, "port3"));
        return conf;
    }

    virtual void save_config(const cluster_config&)
    {
    }
    virtual void save_state(const srv_state&)
    {
    }
    virtual ptr<srv_state> read_state()
    {
        return cs_new<srv_state>();
    }

    virtual ptr<log_store> load_log_store()
    {
        return cs_new<in_mem_log_store>();
    }

    virtual int32 server_id()
    {
        return srv_id_;
    }

    virtual void system_exit(const int exit_code)
    {
        std::cout << "system exiting with code " << exit_code << std::endl;
    }

private:
    int32 srv_id_;
};

class dummy_state_machine : public state_machine
{
public:
    virtual void commit(const ulong, buffer& data, const uptr<log_entry_cookie>&)
    {
        std::cout << "commit message:" << reinterpret_cast<const char*>(data.data()) << std::endl;
    }

    virtual void pre_commit(const ulong, buffer&, const uptr<log_entry_cookie>&)
    {
    }
    virtual void rollback(const ulong, buffer&, const uptr<log_entry_cookie>&)
    {
    }
    virtual void save_snapshot_data(snapshot&, const ulong, buffer&)
    {
    }
    virtual bool apply_snapshot(snapshot&)
    {
        return false;
    }

    virtual int read_snapshot_data(snapshot&, const ulong, buffer&)
    {
        return 0;
    }

    virtual ptr<snapshot> last_snapshot()
    {
        return ptr<snapshot>();
    }

    virtual ulong last_commit_index()
    {
        return 0;
    }

    virtual void create_snapshot(snapshot&, async_result<bool>::handler_type&)
    {
    }
};

class msg_bus
{
public:
    typedef std::pair<ptr<req_msg>, ptr<async_result<ptr<resp_msg>>>> message;

    class msg_queue
    {
    public:
        msg_queue() : queue_(), cv_(), lock_()
        {
        }
        __nocopy__(msg_queue) public : void enqueue(const message& msg)
        {
            {
                auto_lock(lock_);
                queue_.push(msg);
            }

            cv_.notify_one();
        }

        message dequeue()
        {
            std::unique_lock<std::mutex> u_lock(lock_);
            if (queue_.size() == 0)
            {
                cv_.wait(u_lock);
            }

            message& front = queue_.front();
            message m = std::make_pair(front.first, front.second);
            queue_.pop();
            return m;
        }

    private:
        std::queue<message> queue_;
        std::condition_variable cv_;
        std::mutex lock_;
    };

    typedef std::unordered_map<std::string, std::shared_ptr<msg_queue>> msg_q_map;

public:
    msg_bus() : q_map_()
    {
        q_map_.insert(std::make_pair("port1", std::shared_ptr<msg_queue>(new msg_queue)));
        q_map_.insert(std::make_pair("port2", std::shared_ptr<msg_queue>(new msg_queue)));
        q_map_.insert(std::make_pair("port3", std::shared_ptr<msg_queue>(new msg_queue)));
    }

    __nocopy__(msg_bus)

        public : void send_msg(const std::string& port, message& msg)
    {
        msg_q_map::const_iterator itor = q_map_.find(port);
        if (itor != q_map_.end())
        {
            itor->second->enqueue(msg);
            return;
        }

        // this is for test usage, if port is not found, faulted
        throw std::runtime_error(sstrfmt("bad port %s for msg_bus").fmt(port.c_str()));
    }

    msg_queue& get_queue(const std::string& port)
    {
        msg_q_map::iterator itor = q_map_.find(port);
        if (itor == q_map_.end())
        {
            throw std::runtime_error("bad port for msg_bus, no queue found.");
        }

        return *itor->second;
    }

private:
    msg_q_map q_map_;
};

class test_rpc_client : public rpc_client
{
public:
    test_rpc_client(msg_bus& bus, const std::string& port) : bus_(bus), port_(port)
    {
    }

    __nocopy__(test_rpc_client)

        virtual void send(ptr<req_msg>& req, rpc_handler& when_done) __override__
    {
        ptr<async_result<ptr<resp_msg>>> result(cs_new<async_result<ptr<resp_msg>>>());
        result->when_ready(
            [req, when_done](ptr<resp_msg>& resp, const ptr<std::exception>& err) -> void
            {
                if (err != nilptr)
                {
                    ptr<rpc_exception> excep(cs_new<rpc_exception>(err->what(), req));
                    ptr<resp_msg> no_resp;
                    when_done(no_resp, excep);
                }
                else
                {
                    ptr<rpc_exception> no_excep;
                    when_done(resp, no_excep);
                }
            });

        // TODO need to do deep copy of the req to avoid all instances sharing the same request object
        ptr<req_msg> dup_req(cs_new<req_msg>(
            req->get_term(),
            req->get_type(),
            req->get_src(),
            req->get_dst(),
            req->get_last_log_term(),
            req->get_last_log_idx(),
            req->get_commit_idx()));
        for (std::vector<ptr<log_entry>>::const_iterator it = req->log_entries().begin();
             it != req->log_entries().end();
             ++it)
        {
            bufptr buf = buffer::copy((*it)->get_buf());
            ptr<log_entry> entry(cs_new<log_entry>((*it)->get_term(), std::move(buf), (*it)->get_val_type()));
            dup_req->log_entries().push_back(entry);
        }

        msg_bus::message msg(std::make_pair(dup_req, result));
        bus_.send_msg(port_, msg);
    }

private:
    msg_bus& bus_;
    std::string port_;
};

class test_rpc_cli_factory : public rpc_client_factory
{
public:
    test_rpc_cli_factory(msg_bus& bus) : bus_(bus)
    {
    }
    __nocopy__(test_rpc_cli_factory) public
        : virtual ptr<rpc_client> create_client(const std::string& endpoint) __override__
    {
        return cs_new<test_rpc_client, msg_bus&, const std::string&>(bus_, endpoint);
    }

private:
    msg_bus& bus_;
};

class test_rpc_listener : public rpc_listener
{
public:
    test_rpc_listener(const std::string& port, msg_bus& bus)
        : queue_(bus.get_queue(port)), stopped_(false), stop_lock_(), stopped_cv_()
    {
    }
    __nocopy__(test_rpc_listener) public : virtual void listen(ptr<msg_handler>& handler) __override__
    {
        std::thread t([this, handler]() mutable { this->do_listening(handler); });
        t.detach();
    }

    virtual void stop() override
    {
        stopped_ = true;
        queue_.enqueue(std::make_pair(ptr<req_msg>(), ptr<async_result<ptr<resp_msg>>>()));
        std::unique_lock<std::mutex> lock(stop_lock_);
        stopped_cv_.wait(lock);
    }

private:
    void do_listening(ptr<msg_handler> handler)
    {
        while (!stopped_)
        {
            msg_bus::message msg = queue_.dequeue();
            if (msg.first == nilptr)
            {
                break;
            }

            ptr<resp_msg> resp = handler->process_req(*(msg.first));
            if (stopped_)
                break;
            ptr<std::exception> no_err;
            msg.second->set_result(resp, no_err);
        }

        {
            auto_lock(stop_lock_);
            stopped_cv_.notify_all();
        }
    }

private:
    msg_bus::msg_queue& queue_;
    bool stopped_;
    std::mutex stop_lock_;
    std::condition_variable stopped_cv_;
};

msg_bus bus;
ptr<rpc_client_factory> rpc_factory(cs_new<test_rpc_cli_factory, msg_bus&>(bus));
std::condition_variable stop_cv;
std::mutex lock;
ptr<asio_service> asio_svc(cs_new<asio_service>());
std::mutex stop_test_lock;
std::condition_variable stop_test_cv;

void run_raft_instance(int srv_id);
void test_raft_server()
{
    std::thread t1([] { run_raft_instance(1); });
    t1.detach();
    std::thread t2([] { run_raft_instance(2); });
    t2.detach();
    std::thread t3([] { run_raft_instance(3); });
    t3.detach();
    std::cout << "waiting for leader election..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ptr<rpc_client> client(rpc_factory->create_client("port1"));
    ptr<req_msg> msg = cs_new<req_msg>(0, msg_type::client_request, 0, 1, 0, 0, 0);
    bufptr buf = buffer::alloc(100);
    buf->put("hello");
    buf->pos(0);
    msg->log_entries().push_back(cs_new<log_entry>(0, std::move(buf)));
    rpc_handler handler = (rpc_handler)([&client](ptr<resp_msg>& rsp, const ptr<rpc_exception>&) -> void {
        assert(rsp->get_accepted() || rsp->get_dst() > 0);
        if (!rsp->get_accepted()) {
            client = rpc_factory->create_client(sstrfmt("port%d").fmt(rsp->get_dst()));
            ptr<req_msg> msg = cs_new<req_msg>(0, msg_type::client_request, 0, 1, 0, 0, 0);
            bufptr buf = buffer::alloc(100);
            buf->put("hello");
            buf->pos(0);
            msg->log_entries().push_back(cs_new<log_entry>(0, std::move(buf)));
            rpc_handler handler = (rpc_handler)([client](ptr<resp_msg>& rsp1, const ptr<rpc_exception>&) -> void {
                assert(rsp1->get_accepted());
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                stop_test_cv.notify_all();
            });
            client->send(msg, handler);
        }
        else {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            stop_test_cv.notify_all();
        }
    });

    client->send(msg, handler);

    {
        std::unique_lock<std::mutex> l(stop_test_lock);
        stop_test_cv.wait(l);
    }

    stop_cv.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    asio_svc->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::remove("log1.log");
    std::remove("log2.log");
    std::remove("log3.log");
}

void run_raft_instance(int srv_id)
{
    ptr<rpc_listener> listener(
        cs_new<test_rpc_listener, const std::string&, msg_bus&>(sstrfmt("port%d").fmt(srv_id), bus));
    ptr<state_mgr> smgr(cs_new<in_memory_state_mgr>(srv_id));
    ptr<state_machine> smachine(cs_new<dummy_state_machine>());
    ptr<logger> l(asio_svc->create_logger(asio_service::log_level::debug, sstrfmt("log%d.log").fmt(srv_id)));
    raft_params* params(new raft_params());
    (*params)
        .with_election_timeout_lower(200)
        .with_election_timeout_upper(400)
        .with_hb_interval(100)
        .with_max_append_size(100)
        .with_rpc_failure_backoff(50);
    ptr<delayed_task_scheduler> scheduler = asio_svc;
    context* ctx(new context(smgr, smachine, listener, l, rpc_factory, scheduler, nilptr, params));
    ptr<raft_server> server(cs_new<raft_server>(ctx));
    listener->listen(server);

    // some example code for how to append log entries to raft_server
    /*std::this_thread::sleep_for(std::chrono::seconds(1));
    bufptr buf(buffer::alloc(100));
    buf->put(sstrfmt("log from srv %d").fmt(srv_id));
    buf->pos(0);
    std::vector<bufptr> logs;
    logs.emplace_back(std::move(buf));
    ptr<async_result<bool>> presult = server->append_entries(logs);
    */

    {
        std::unique_lock<std::mutex> ulock(lock);
        stop_cv.wait(ulock);
        listener->stop();
    }
}
