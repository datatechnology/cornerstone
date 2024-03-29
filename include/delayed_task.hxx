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

#ifndef _DELAYED_TASK_HXX_
#define _DELAYED_TASK_HXX_

#include <atomic>
#include <functional>
#include "basic_types.hxx"
#include "pp_util.hxx"

namespace cornerstone
{
class delayed_task
{
public:
    delayed_task() : cancelled_(false), impl_ctx_(nilptr), impl_ctx_del_()
    {
    }
    virtual ~delayed_task()
    {
        if (impl_ctx_ != nilptr)
        {
            if (impl_ctx_del_)
            {
                impl_ctx_del_(impl_ctx_);
            }
        }
    }

    __nocopy__(delayed_task);

public:
    void execute()
    {
        if (!cancelled_.load())
        {
            exec();
        }
    }

    void cancel()
    {
        cancelled_.store(true);
    }

    void reset()
    {
        cancelled_.store(false);
    }

    void* get_impl_context() const
    {
        return impl_ctx_;
    }

    void set_impl_context(void* ctx, std::function<void(void*)> del)
    {
        impl_ctx_ = ctx;
        impl_ctx_del_ = del;
    }

protected:
    virtual void exec() = 0;

private:
    std::atomic<bool> cancelled_;
    void* impl_ctx_;
    std::function<void(void*)> impl_ctx_del_;
};
} // namespace cornerstone

#endif