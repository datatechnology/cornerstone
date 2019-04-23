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

#ifndef _ASYNC_HXX_
#define _ASYNC_HXX_

namespace cornerstone {
    template<typename T, typename TE = ptr<std::exception>>
    class async_result {
    public:
        typedef std::function<void(T&, const TE&)> handler_type;
        async_result() : err_(), has_result_(false), lock_(), cv_() {}
        explicit async_result(T& result)
            : result_(result), err_(), has_result_(true), lock_(), cv_() {}
        explicit async_result(handler_type& handler)
            : err_(), has_result_(true), handler_(handler), lock_(), cv_() {}

        ~async_result() {}

        __nocopy__(async_result)

    public:
        template<typename _THandler>
        void when_ready(_THandler&& handler) {
            {
                std::lock_guard<std::mutex> guard(lock_);
                handler_ = std::forward<_THandler>(handler);
            }

            if (has_result_) {
                handler_(result_, err_);
            }
        }

        template<typename _TResult, typename _TException>
        void set_result(_TResult&& result, _TException&& err) {
            handler_type handler;
	        {
                std::lock_guard<std::mutex> guard(lock_);
                result_ = std::forward<_TResult>(result);
                err_ = std::forward<_TException>(err);
                has_result_ = true;
                if (handler_)
                {
                    handler = handler_; // copy handler as std::function assignment is not atomic guaranteed
                }
	        }

            if (handler) {
	            handler(result_, err_);
	        }

            cv_.notify_all();
        }

        T& get() {
            std::unique_lock<std::mutex> lock(lock_);
            if (has_result_) {
                if (err_ == nullptr) {
                    return result_;
                }

                throw err_;
            }

            cv_.wait(lock);
            if (err_ == nullptr) {
                return result_;
            }

            throw err_;
        }

    private:
        T result_;
        TE err_;
        volatile bool has_result_;
        handler_type handler_;
        std::mutex lock_;
        std::condition_variable cv_;
    };
}

#endif //_ASYNC_HXX_
