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

#ifndef _TIMER_TASK_HXX_
#define _TIMER_TASK_HXX_

#include "delayed_task.hxx"

namespace cornerstone
{
template <typename T>
class timer_task : public delayed_task
{
public:
    using executor = std::function<void(T)>;

    timer_task(executor& e, T ctx) : exec_(e), ctx_(ctx)
    {
    }

protected:
    virtual void exec() __override__
    {
        if (exec_)
        {
            exec_(ctx_);
        }
    }

private:
    executor exec_;
    T ctx_;
};

template <>
class timer_task<void> : public delayed_task
{
public:
    typedef std::function<void()> executor;

    explicit timer_task(executor& e) : exec_(e)
    {
    }

protected:
    virtual void exec() __override__
    {
        if (exec_)
        {
            exec_();
        }
    }

private:
    executor exec_;
};
} // namespace cornerstone

#endif //_TIMER_TASK_HXX_
