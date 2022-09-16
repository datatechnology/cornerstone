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
#include "cornerstone.hxx"


using namespace cornerstone;

extern void cleanup(const std::string& folder);

#ifdef _WIN32
extern int mkdir(const char* path, int mode);
extern int rmdir(const char* path);
#else
#define LOG_INDEX_FILE "/store.idx"
#define LOG_DATA_FILE "/store.dat"
#define LOG_START_INDEX_FILE "/store.sti"
#define LOG_INDEX_FILE_BAK "/store.idx.bak"
#define LOG_DATA_FILE_BAK "/store.dat.bak"
#define LOG_START_INDEX_FILE_BAK "/store.sti.bak"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#endif

ptr<asio_service> asio_svc_;
std::condition_variable stop_cv1;
std::mutex lock1;
std::mutex stop_test_lock1;
std::condition_variable stop_test_cv1;

class simple_state_mgr : public state_mgr
{
public:
    simple_state_mgr(int32 srv_id, const std::vector<ptr<srv_config>>& cluster) : srv_id_(srv_id), cluster_(cluster)
    {
        store_path_ = sstrfmt("store%d").fmt(srv_id_);
    }

public:
    virtual ptr<cluster_config> load_config()
    {
        ptr<cluster_config> conf = cs_new<cluster_config>();
        for (const auto& srv : cluster_)
        {
            conf->get_servers().push_back(srv);
        }

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
        mkdir(store_path_.c_str(), 0766);
        return cs_new<fs_log_store>(store_path_);
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
    std::vector<ptr<srv_config>> cluster_;
    std::string store_path_;
};

class fs_logger : public logger
{
public:
    fs_logger(const std::string& filename) : fs_(filename)
    {
    }

    __nocopy__(fs_logger);

public:
    virtual void debug(const std::string& log_line)
    {
        fs_ << log_line << std::endl;
        fs_.flush();
    }

    virtual void info(const std::string& log_line)
    {
        fs_ << log_line << std::endl;
        fs_.flush();
    }

    virtual void warn(const std::string& log_line)
    {
        fs_ << log_line << std::endl;
        fs_.flush();
    }

    virtual void err(const std::string& log_line)
    {
        fs_ << log_line << std::endl;
        fs_.flush();
    }

private:
    std::ofstream fs_;
};

class console_logger : public logger
{
public:
    console_logger(const std::string& name) : name_(name)
    {
    }

    __nocopy__(console_logger) public : virtual void debug(const std::string& log_line)
    {
        printf("%s %s %s\n", "DEBUG", this->name_.c_str(), log_line.c_str());
    }

    virtual void info(const std::string& log_line)
    {
        printf("%s %s %s\n", "INFO", this->name_.c_str(), log_line.c_str());
    }

    virtual void warn(const std::string& log_line)
    {
        printf("%s %s %s\n", "WARN", this->name_.c_str(), log_line.c_str());
    }

    virtual void err(const std::string& log_line)
    {
        printf("%s %s %s\n", "ERROR", this->name_.c_str(), log_line.c_str());
    }

private:
    std::string name_;
};

class echo_state_machine : public state_machine
{
public:
    echo_state_machine() : lock_()
    {
    }

public:
    virtual void commit(const ulong, buffer& data, const uptr<log_entry_cookie>&)
    {
        auto_lock(lock_);
        std::cout << "commit message:" << reinterpret_cast<const char*>(data.data()) << std::endl;
    }

    virtual void pre_commit(const ulong, buffer& data, const uptr<log_entry_cookie>&)
    {
        auto_lock(lock_);
        std::cout << "pre-commit: " << reinterpret_cast<const char*>(data.data()) << std::endl;
    }

    virtual void rollback(const ulong, buffer& data, const uptr<log_entry_cookie>&)
    {
        auto_lock(lock_);
        std::cout << "rollback: " << reinterpret_cast<const char*>(data.data()) << std::endl;
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

    virtual ulong last_commit_index()
    {
        return 0;
    }

    virtual ptr<snapshot> last_snapshot()
    {
        return ptr<snapshot>();
    }

    virtual void create_snapshot(snapshot&, async_result<bool>::handler_type&)
    {
    }

private:
    std::mutex lock_;
};

void run_raft_instance_with_asio(int srv_id, bool enable_prevote, const std::vector<ptr<srv_config>>& cluster);
void test_raft_server_with_asio()
{
    asio_svc_ = cs_new<asio_service>();
    auto cluster = std::vector<ptr<srv_config>>(
        {cs_new<srv_config>(1, "tcp://127.0.0.1:9001"),
         cs_new<srv_config>(2, "tcp://127.0.0.1:9002"),
         cs_new<srv_config>(3, "tcp://127.0.0.1:9003")});
    std::thread t1([&cluster] { run_raft_instance_with_asio(1, false, cluster); });
    t1.detach();
    std::thread t2([&cluster] { run_raft_instance_with_asio(2, false, cluster); });
    t2.detach();
    std::thread t3([&cluster] { run_raft_instance_with_asio(3, false, cluster); });
    t3.detach();
    std::cout << "waiting for leader election..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ptr<rpc_client> client(asio_svc_->create_client("tcp://127.0.0.1:9001"));
    ptr<req_msg> msg = cs_new<req_msg>(0, msg_type::client_request, 0, 1, 0, 0, 0);
    bufptr buf = buffer::alloc(100);
    buf->put("hello");
    buf->pos(0);
    msg->log_entries().push_back(cs_new<log_entry>(0, std::move(buf)));
    rpc_handler handler = (rpc_handler)([&client](ptr<resp_msg>& rsp, const ptr<rpc_exception>& err) -> void {
        if (err) {
            std::cout << err->what() << std::endl;
        }

        assert(!err);
        assert(rsp->get_accepted() || rsp->get_dst() > 0);
        if (!rsp->get_accepted()) {
            client = asio_svc_->create_client(sstrfmt("tcp://127.0.0.1:900%d").fmt(rsp->get_dst()));
            ptr<req_msg> msg = cs_new<req_msg>(0, msg_type::client_request, 0, 1, 0, 0, 0);
            bufptr buf = buffer::alloc(100);
            buf->put("hello");
            buf->pos(0);
            msg->log_entries().push_back(cs_new<log_entry>(0, std::move(buf)));
            rpc_handler handler = (rpc_handler)([client](ptr<resp_msg>& rsp1, const ptr<rpc_exception>&) -> void {
                assert(rsp1->get_accepted());
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                stop_test_cv1.notify_all();
            });
            client->send(msg, handler);
        }
        else {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            stop_test_cv1.notify_all();
        }
    });

    client->send(msg, handler);

    {
        std::unique_lock<std::mutex> l(stop_test_lock1);
        stop_test_cv1.wait(l);
    }

    stop_cv1.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    asio_svc_->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::remove("log1.log");
    std::remove("log2.log");
    std::remove("log3.log");
    cleanup("store1");
    cleanup("store2");
    cleanup("store3");
    rmdir("store1");
    rmdir("store2");
    rmdir("store3");
}

void test_raft_server_with_prevote()
{
    asio_svc_ = cs_new<asio_service>();
    auto cluster = std::vector<ptr<srv_config>>(
        {cs_new<srv_config>(4, "tcp://127.0.0.1:9004"),
         cs_new<srv_config>(5, "tcp://127.0.0.1:9005"),
         cs_new<srv_config>(6, "tcp://127.0.0.1:9006")});
    std::thread t1([cluster] { run_raft_instance_with_asio(4, true, cluster); });
    t1.detach();
    std::thread t2([cluster] { run_raft_instance_with_asio(5, true, cluster); });
    t2.detach();
    std::thread t3([cluster] { run_raft_instance_with_asio(6, true, cluster); });
    t3.detach();
    std::cout << "waiting for leader election..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ptr<rpc_client> client(asio_svc_->create_client("tcp://127.0.0.1:9004"));
    ptr<req_msg> msg = cs_new<req_msg>(0, msg_type::client_request, 0, 1, 0, 0, 0);
    bufptr buf = buffer::alloc(100);
    buf->put("hello");
    buf->pos(0);
    msg->log_entries().push_back(cs_new<log_entry>(0, std::move(buf)));
    rpc_handler handler = (rpc_handler)([&client](ptr<resp_msg>& rsp, const ptr<rpc_exception>& err) -> void {
        if (err) {
            std::cout << err->what() << std::endl;
        }

        assert(!err);
        assert(rsp->get_accepted() || rsp->get_dst() > 0);
        if (!rsp->get_accepted()) {
            client = asio_svc_->create_client(sstrfmt("tcp://127.0.0.1:900%d").fmt(rsp->get_dst()));
            ptr<req_msg> msg = cs_new<req_msg>(0, msg_type::client_request, 0, 1, 0, 0, 0);
            bufptr buf = buffer::alloc(100);
            buf->put("hello");
            buf->pos(0);
            msg->log_entries().push_back(cs_new<log_entry>(0, std::move(buf)));
            rpc_handler handler = (rpc_handler)([client](ptr<resp_msg>& rsp1, const ptr<rpc_exception>&) -> void {
                assert(rsp1->get_accepted());
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                stop_test_cv1.notify_all();
            });
            client->send(msg, handler);
        }
        else {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            stop_test_cv1.notify_all();
        }
    });

    client->send(msg, handler);

    {
        std::unique_lock<std::mutex> l(stop_test_lock1);
        stop_test_cv1.wait(l);
    }

    stop_cv1.notify_all();
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    asio_svc_->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::remove("log4.log");
    std::remove("log5.log");
    std::remove("log6.log");
    cleanup("store4");
    cleanup("store5");
    cleanup("store6");
    rmdir("store4");
    rmdir("store5");
    rmdir("store6");
}

class test_event_listener : public raft_event_listener
{
public:
    test_event_listener(int id) : raft_event_listener(), srv_id_(id)
    {
    }

public:
    virtual void on_event(raft_event event) override
    {
        switch (event)
        {
            case raft_event::become_follower:
                std::cout << srv_id_ << " becomes a follower" << std::endl;
                break;
            case raft_event::become_leader:
                std::cout << srv_id_ << " becomes a leader" << std::endl;
                break;
            case raft_event::logs_catch_up:
                std::cout << srv_id_ << " catch up all logs" << std::endl;
                break;
        }
    }

private:
    int srv_id_;
};

void run_raft_instance_with_asio(int srv_id, bool enable_prevote, const std::vector<ptr<srv_config>>& cluster)
{
    ptr<logger> l(cs_new<fs_logger>(sstrfmt("log%d.log").fmt(srv_id)));
    ptr<rpc_listener> listener(asio_svc_->create_rpc_listener((ushort)(9000 + srv_id), l));
    ptr<state_mgr> smgr(cs_new<simple_state_mgr>(srv_id, cluster));
    ptr<state_machine> smachine(cs_new<echo_state_machine>());
    raft_params* params(new raft_params());
    (*params)
        .with_election_timeout_lower(200)
        .with_election_timeout_upper(400)
        .with_hb_interval(100)
        .with_max_append_size(100)
        .with_rpc_failure_backoff(50)
        .with_prevote_enabled(enable_prevote);
    ptr<delayed_task_scheduler> scheduler = asio_svc_;
    ptr<rpc_client_factory> rpc_cli_factory = asio_svc_;
    context* ctx(new context(
        smgr, smachine, listener, l, rpc_cli_factory, scheduler, cs_new<test_event_listener>(srv_id), params));
    ptr<raft_server> server(cs_new<raft_server>(ctx));
    listener->listen(server);

    {
        std::unique_lock<std::mutex> ulock(lock1);
        stop_cv1.wait(ulock);
        listener->stop();
    }
}