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

#define ASIO_STANDALONE 1
#define ASIO_HAS_STD_CHRONO 1
#if defined(__EDG_VERSION__)
#undef __EDG_VERSION__
#endif

#define  _CRT_SECURE_NO_WARNINGS

#include "../include/cornerstone.hxx"
#include "../asio/asio/include/asio.hpp"
#include <fstream>
#include <queue>
#include <ctime>
#include <regex>

// request header, ulong term (8), msg_type type (1), int32 src (4), int32 dst (4), ulong last_log_term (8), ulong last_log_idx (8), ulong commit_idx (8) + one int32 (4) for log data size 
#define RPC_REQ_HEADER_SIZE 3 * 4 + 8 * 4 + 1
 
// response header ulong term (8), msg_type type (1), int32 src (4), int32 dst (4), ulong next_idx (8), bool accepted (1)
#define RPC_RESP_HEADER_SIZE 4 * 2 + 8 * 2 + 2

namespace cornerstone {

    namespace impls {
        // logger implementation
        class fs_based_logger : public logger {
        public:
            fs_based_logger(const std::string& log_file, cornerstone::asio_service::log_level level)
                : level_(level), fs_(log_file), buffer_(), lock_() {}

            virtual ~fs_based_logger();

            __nocopy__(fs_based_logger)
        public:
            virtual void debug(const std::string& log_line) {
                if (level_ <= 0) {
                    write_log("dbug", log_line);
                }
            }

            virtual void info(const std::string& log_line) {
                if (level_ <= 1) {
                    write_log("info", log_line);
                }
            }

            virtual void warn(const std::string& log_line) {
                if (level_ <= 2) {
                    write_log("warn", log_line);
                }
            }

            virtual void err(const std::string& log_line) {
                if (level_ <= 3) {
                    write_log("errr", log_line);
                }
            }

            void flush();
        private:
            void write_log(const std::string& level, const std::string& log_line);
        private:
            cornerstone::asio_service::log_level level_;
            std::ofstream fs_;
            std::queue<std::string> buffer_;
            std::mutex lock_;
        };
    }

    // asio service implementation
    class asio_service_impl {
    public:
        asio_service_impl();
        ~asio_service_impl();

    private:
        void stop();
        void worker_entry();
        void flush_all_loggers(asio::error_code err);

    private:
        asio::io_service io_svc_;
        asio::steady_timer log_flush_tm_;
        std::atomic_int continue_;
        std::mutex logger_list_lock_;
        std::list<ptr<impls::fs_based_logger>> loggers_;
	    bool stopping_;
	    std::mutex stopping_lock_;
	    std::condition_variable stopping_cv_;
        friend asio_service;
	    friend impls::fs_based_logger;
    };

    // rpc session 
    class rpc_session;
    typedef std::function<void(const ptr<rpc_session>&)> session_closed_callback;

    class rpc_session : public std::enable_shared_from_this<rpc_session> {
    public:
        template<typename SessionCloseCallback>
        rpc_session(asio::io_service& io, ptr<msg_handler>& handler, ptr<logger>& logger, SessionCloseCallback&& callback)
            : handler_(handler), socket_(io), log_data_(buffer::alloc(0)), header_(buffer::alloc(RPC_REQ_HEADER_SIZE)), l_(logger), callback_(std::move(callback)) {}

        __nocopy__(rpc_session)

    public:
        ~rpc_session() {
            if (socket_.is_open()) {
                socket_.close();
            }
        }

    public:
        void start() {
            ptr<rpc_session> self = this->shared_from_this(); // this is safe since we only expose ctor to cs_new
            asio::async_read(socket_, asio::buffer(header_->data(), RPC_REQ_HEADER_SIZE), [this, self](const asio::error_code& err, size_t) -> void {
                if (!err) {
                    header_->pos(RPC_REQ_HEADER_SIZE - 4);
                    int32 data_size = header_->get_int();
                    if (data_size < 0 || data_size > 0x1000000) {
                        l_->warn(lstrfmt("bad log data size in the header %d, stop this session to protect further corruption").fmt(data_size));
                        this->stop();
                        return;
                    }

                    if (data_size == 0) {
                        this->read_complete();
                        return;
                    }

                    this->log_data_ = buffer::alloc((size_t)data_size);
                    asio::async_read(this->socket_, asio::buffer(this->log_data_->data(), (size_t)data_size), std::bind(&rpc_session::read_log_data, self, std::placeholders::_1, std::placeholders::_2));
                }
                else {
                    l_->err(lstrfmt("failed to read rpc header from socket due to error %d").fmt(err.value()));
                    this->stop();
                }
            });
        }

        void stop() {
            socket_.close();
            if (callback_) {
                callback_(this->shared_from_this());
            }
        }

        asio::ip::tcp::socket& socket() {
            return socket_;
        }

    private:
        void read_log_data(const asio::error_code& err, size_t /* bytes_read */) {
            if (!err) {
                this->read_complete();
            }
            else {
                l_->err(lstrfmt("failed to read rpc log data from socket due to error %d").fmt(err.value()));
                this->stop();
            }
        }

        void read_complete() {
            ptr<rpc_session> self = this->shared_from_this();
            try {
                header_->pos(0);
                msg_type t = (msg_type)header_->get_byte();
                int32 src = header_->get_int();
                int32 dst = header_->get_int();
                ulong term = header_->get_ulong();
                ulong last_term = header_->get_ulong();
                ulong last_idx = header_->get_ulong();
                ulong commit_idx = header_->get_ulong();
                ptr<req_msg> req(cs_new<req_msg>(term, t, src, dst, last_term, last_idx, commit_idx));
                if (header_->get_int() > 0 && log_data_) {
                    log_data_->pos(0);
                    while (log_data_->size() > log_data_->pos()) {
                        ulong term = log_data_->get_ulong();
                        log_val_type val_type = (log_val_type)log_data_->get_byte();
                        int32 val_size = log_data_->get_int();
                        bufptr buf(buffer::alloc((size_t)val_size));
                        log_data_->get(buf);
                        ptr<log_entry> entry(cs_new<log_entry>(term, std::move(buf), val_type));
                        req->log_entries().push_back(entry);
                    }
                }

                ptr<resp_msg> resp = handler_->process_req(*req);
                if (!resp) {
                    l_->err("no response is returned from raft message handler, potential system bug");
                    this->stop();
                }
                else {
                    bufptr resp_buf(buffer::alloc(RPC_RESP_HEADER_SIZE));
                    resp_buf->put((byte)resp->get_type());
                    resp_buf->put(resp->get_src());
                    resp_buf->put(resp->get_dst());
                    resp_buf->put(resp->get_term());
                    resp_buf->put(resp->get_next_idx());
                    resp_buf->put((byte)resp->get_accepted());
                    resp_buf->pos(0);
                    auto buffer = asio::buffer(resp_buf->data(), RPC_RESP_HEADER_SIZE);
                    asio::async_write(socket_, std::move(buffer), [this, self, buf = std::move(resp_buf)](asio::error_code err_code, size_t) -> void {
                        if (!err_code) {
                            this->header_->pos(0);
                            this->start();
                        }
                        else {
                            this->l_->err(lstrfmt("failed to send response to peer due to error %d").fmt(err_code.value()));
                            this->stop();
                        }
                    });
                }
            }
            catch (std::exception& ex) {
                l_->err(lstrfmt("failed to process request message due to error: %s").fmt(ex.what()));
                this->stop();
            }
        }

    private:
        ptr<msg_handler> handler_;
        asio::ip::tcp::socket socket_;
        bufptr log_data_;
        bufptr header_;
        ptr<logger> l_;
        session_closed_callback callback_;
    };
    // rpc listener implementation
    class asio_rpc_listener : public rpc_listener, public std::enable_shared_from_this<asio_rpc_listener> {
    public:
        asio_rpc_listener(asio::io_service& io, ushort port, ptr<logger>& l) 
            : io_svc_(io), handler_(), acceptor_(io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)), active_sessions_(), session_lock_(), l_(l) {}
        __nocopy__(asio_rpc_listener)

    public:
        virtual void stop() override {
            acceptor_.close();
        }

        virtual void listen(ptr<msg_handler>& handler) override {
            handler_ = handler;
            start();
        }

    private:
        void start() {
            if (!acceptor_.is_open()) {
                return;
            }

            ptr<asio_rpc_listener> self(this->shared_from_this());
            ptr<rpc_session> session(cs_new<rpc_session>(io_svc_, handler_, l_, std::bind(&asio_rpc_listener::remove_session, self, std::placeholders::_1)));
            acceptor_.async_accept(session->socket(), [self, this, session](const asio::error_code& err) -> void {
                if (!err) {
                    this->l_->debug("receive a incoming rpc connection");
                    session->start();
                }
                else {
                    this->l_->debug(sstrfmt("fails to accept a rpc connection due to error %d").fmt(err.value()));
                }

                this->start();
            });
        }
        void remove_session(const ptr<rpc_session>& session) {
            auto_lock(session_lock_);
            for (std::vector<ptr<rpc_session>>::iterator it = active_sessions_.begin(); it != active_sessions_.end(); ++it) {
                if (*it == session) {
                    active_sessions_.erase(it);
                    break;
                }
            }
        }

    private:
        asio::io_service& io_svc_;
        ptr<msg_handler> handler_;
        asio::ip::tcp::acceptor acceptor_;
        std::vector<ptr<rpc_session>> active_sessions_;
        std::mutex session_lock_;
        ptr<logger> l_;
    };

    class asio_rpc_client : public rpc_client, public std::enable_shared_from_this<asio_rpc_client> {
    public:
        asio_rpc_client(asio::io_service& io_svc, std::string& host, std::string& port)
            : resolver_(io_svc), socket_(io_svc), host_(host), port_(port) {}
        virtual ~asio_rpc_client() {
            if (socket_.is_open()) {
                socket_.close();
            }
        }

    public:
        virtual void send(ptr<req_msg>& req, rpc_handler& when_done) __override__ {
            ptr<asio_rpc_client> self(this->shared_from_this());
            if (!socket_.is_open()) {
                asio::ip::tcp::resolver::query q(host_, port_, asio::ip::tcp::resolver::query::all_matching);
                resolver_.async_resolve(q, [self, this, req, when_done](std::error_code err, asio::ip::tcp::resolver::iterator itor) -> void {
                    if (!err) {
                        asio::async_connect(socket_, itor, std::bind(&asio_rpc_client::connected, self, req, when_done, std::placeholders::_1, std::placeholders::_2));
                    }
                    else {
                        ptr<resp_msg> rsp;
                        ptr<rpc_exception> except(cs_new<rpc_exception>(lstrfmt("failed to resolve host %s due to error %d").fmt(host_.c_str(), err.value()), req));
                        when_done(rsp, except);
                    }
                });
            } else {
                // serialize req, send and read response
                std::vector<bufptr> log_entry_bufs;
                int32 log_data_size(0);
                for (std::vector<ptr<log_entry>>::const_iterator it = req->log_entries().begin();
                    it != req->log_entries().end(); 
                    ++it) {
                    bufptr entry_buf(buffer::alloc(8 + 1 + 4 + (*it)->get_buf().size()));
                    entry_buf->put((*it)->get_term());
                    entry_buf->put((byte)((*it)->get_val_type()));
                    entry_buf->put((int32)(*it)->get_buf().size());
                    (*it)->get_buf().pos(0);
                    entry_buf->put((*it)->get_buf());
                    entry_buf->pos(0);
                    log_data_size += (int32)entry_buf->size();
                    log_entry_bufs.emplace_back(std::move(entry_buf));
                }

                bufptr req_buf(buffer::alloc(RPC_REQ_HEADER_SIZE + log_data_size));
                req_buf->put((byte)req->get_type());
                req_buf->put(req->get_src());
                req_buf->put(req->get_dst());
                req_buf->put(req->get_term());
                req_buf->put(req->get_last_log_term());
                req_buf->put(req->get_last_log_idx());
                req_buf->put(req->get_commit_idx());
                req_buf->put(log_data_size);
                for (auto& item : log_entry_bufs) {
                    req_buf->put(*item);
                }

                req_buf->pos(0);
                auto buffer = asio::buffer(req_buf->data(), req_buf->size());
                asio::async_write(socket_, std::move(buffer), [self, req_msg = req, when_done, buf = std::move(req_buf)](std::error_code err, size_t bytes_transferred) mutable -> void
                {
                    self->sent(req_msg, buf, when_done, err, bytes_transferred);
                });
            }
        }
    private:
        void connected(ptr<req_msg>& req, rpc_handler& when_done, std::error_code err, asio::ip::tcp::resolver::iterator /* itor */) {
            if (!err) {
                this->send(req, when_done);
            }
            else {
                ptr<resp_msg> rsp;
                ptr<rpc_exception> except(cs_new<rpc_exception>(sstrfmt("failed to connect to remote socket %d").fmt(err.value()), req));
                when_done(rsp, except);
            }
        }

        void sent(ptr<req_msg>& req, bufptr& /* buf */, rpc_handler& when_done, std::error_code err, size_t /* bytes_transferred */) {
            ptr<asio_rpc_client> self(this->shared_from_this());
            if (!err) {
                // read a response
                bufptr resp_buf(buffer::alloc(RPC_RESP_HEADER_SIZE));
                auto buffer = asio::buffer(resp_buf->data(), resp_buf->size());
                asio::async_read(socket_, std::move(buffer), [self, req_msg = req, when_done, rbuf = std::move(resp_buf)](std::error_code err, size_t bytes_transferred) mutable -> void
                {
                    self->response_read(req_msg, when_done, rbuf, err, bytes_transferred);
                });
            }
            else {
                ptr<resp_msg> rsp;
                ptr<rpc_exception> except(cs_new<rpc_exception>(sstrfmt("failed to send request to remote socket %d").fmt(err.value()), req));
                socket_.close();
                when_done(rsp, except);
            }
        }

        void response_read(ptr<req_msg>& req, rpc_handler& when_done, bufptr& resp_buf, std::error_code err, size_t /* bytes_transferred */) {
            if (!err) {
                byte msg_type_val = resp_buf->get_byte();
                int32 src = resp_buf->get_int();
                int32 dst = resp_buf->get_int();
                ulong term = resp_buf->get_ulong();
                ulong nxt_idx = resp_buf->get_ulong();
                byte accepted_val = resp_buf->get_byte();
                ptr<resp_msg> rsp(cs_new<resp_msg>(term, (msg_type)msg_type_val, src, dst, nxt_idx, accepted_val == 1));
                ptr<rpc_exception> except;
                when_done(rsp, except);
            }
            else {
                ptr<resp_msg> rsp;
                ptr<rpc_exception> except(cs_new<rpc_exception>(sstrfmt("failed to read response to remote socket %d").fmt(err.value()), req));
                socket_.close();
                when_done(rsp, except);
            }
        }

    private:
        asio::ip::tcp::resolver resolver_;
        asio::ip::tcp::socket socket_;
        std::string host_;
        std::string port_;
    };
}

using namespace cornerstone;
using namespace cornerstone::impls;

void _free_timer_(void* ptr) {
    asio::steady_timer* timer = static_cast<asio::steady_timer*>(ptr);
    delete timer;
}

void _timer_handler_(ptr<delayed_task>& task, asio::error_code err) {
    if (!err) {
        task->execute();
    }
}


asio_service_impl::asio_service_impl()
    : io_svc_(), log_flush_tm_(io_svc_), continue_(1), logger_list_lock_(), loggers_(), stopping_(false), stopping_lock_(), stopping_cv_() {
    // set expires_after to a very large value so that this will not affect the overall performance
    log_flush_tm_.expires_after(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::seconds(1)));
    log_flush_tm_.async_wait(std::bind(&asio_service_impl::flush_all_loggers, this, std::placeholders::_1));
    unsigned int cpu_cnt = std::thread::hardware_concurrency();
    if (cpu_cnt == 0) {
        cpu_cnt = 1;
    }

    for (unsigned int i = 0; i < cpu_cnt; ++i) {
        std::thread t(std::bind(&asio_service_impl::worker_entry, this));
        t.detach();
    }
}

asio_service_impl::~asio_service_impl() {
    stop();
}

void asio_service_impl::worker_entry() {
    try {
        io_svc_.run();
    }
    catch (...) {
        // ignore all exceptions
    }
}

void asio_service_impl::flush_all_loggers(asio::error_code /* err */) {
    if (continue_.load() == 1) {
        log_flush_tm_.expires_after(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::hours(1000)));
        log_flush_tm_.async_wait(std::bind(&asio_service_impl::flush_all_loggers, this, std::placeholders::_1));
    }

    {
        std::lock_guard<std::mutex> guard(logger_list_lock_);
        for (std::list<ptr<fs_based_logger>>::iterator it = loggers_.begin(); it != loggers_.end(); ++it) {
            (*it)->flush();
        }
    }

    if(stopping_){
	    stopping_cv_.notify_all();
    }
}

void asio_service_impl::stop() {
    int running = 1;
    if (continue_.compare_exchange_strong(running, 0)) {
	    std::unique_lock<std::mutex> lock(stopping_lock_);
	    stopping_ = true;
	    log_flush_tm_.cancel();
	    stopping_cv_.wait(lock);
	    lock.unlock();
	    lock.release();
    }
}

void cornerstone::impls::fs_based_logger::flush() {
    std::queue<std::string> backup;
    {
        std::lock_guard<std::mutex> guard(lock_);
        if (buffer_.size() > 0) {
            backup.swap(buffer_);
        }
    }

    while (backup.size() > 0) {
        std::string& line = backup.front();
        fs_ << line << std::endl;
        backup.pop();
    }
}

cornerstone::impls::fs_based_logger::~fs_based_logger() {
    flush();
    fs_.flush();
    fs_.close();
}

void cornerstone::impls::fs_based_logger::write_log(const std::string& level, const std::string& log_line) {
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    int ms = (int)(std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000);
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::tm* tm = std::gmtime(&now_c);
    std::hash<std::thread::id> hasher;
    std::string line(sstrfmt("%d/%d/%d %d:%d:%d.%d\t[%d]\t%s\t").fmt(tm->tm_mon + 1, tm->tm_mday, tm->tm_year + 1900, tm->tm_hour, tm->tm_min, tm->tm_sec, ms, hasher(std::this_thread::get_id()), level.c_str()));
    line += log_line;
    {
        std::lock_guard<std::mutex> guard(lock_);
        buffer_.push(line);
    }
}

asio_service::asio_service()
    : impl_(new asio_service_impl) {
    
}

asio_service::~asio_service() {
    delete impl_;
}

void asio_service::schedule(ptr<delayed_task>& task, int32 milliseconds) {
    if (task->get_impl_context() == nilptr) {
        task->set_impl_context(new asio::steady_timer(impl_->io_svc_), &_free_timer_);
    }
    // ensure it's not in cancelled state
    task->reset();

    asio::steady_timer* timer = static_cast<asio::steady_timer*>(task->get_impl_context());
    timer->expires_after(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds(milliseconds)));
    timer->async_wait(std::bind(&_timer_handler_, task, std::placeholders::_1));
}

void asio_service::cancel_impl(ptr<delayed_task>& task) {
    if (task->get_impl_context() != nilptr) {
        static_cast<asio::steady_timer*>(task->get_impl_context())->cancel();
    }
}

void asio_service::stop() {
    impl_->stop();
}

ptr<rpc_client> asio_service::create_client(const std::string& endpoint) {
    // the endpoint is expecting to be protocol://host:port, and we only support tcp for this factory
    // which is endpoint must be tcp://hostname:port
    static std::regex reg("^tcp://(([a-zA-Z0-9\\-]+\\.)*([a-zA-Z0-9]+)):([0-9]+)$");
    std::smatch mresults;
    if (!std::regex_match(endpoint, mresults, reg) || mresults.size() != 5) {
        return ptr<rpc_client>();
    }

    std::string hostname = mresults[1].str();
    std::string port = mresults[4].str();
    return cs_new<asio_rpc_client, asio::io_service&, std::string&, std::string&>(impl_->io_svc_, hostname, port);
}

ptr<logger> asio_service::create_logger(log_level level, const std::string& log_file) {
    ptr<fs_based_logger> l = cs_new<fs_based_logger>(log_file, level);
    {
        std::lock_guard<std::mutex> guard(impl_->logger_list_lock_);
        impl_->loggers_.push_back(l);
    }

    return l;
}

ptr<rpc_listener> asio_service::create_rpc_listener(ushort listening_port, ptr<logger>& l) {
    return cs_new<asio_rpc_listener>(impl_->io_svc_, listening_port, l);
}
