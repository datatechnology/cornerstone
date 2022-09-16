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

#ifndef _RESP_MSG_HXX_
#define _RESP_MSG_HXX_

#include "msg_base.hxx"

namespace cornerstone
{
class resp_msg : public msg_base
{
public:
    resp_msg(ulong term, msg_type type, int32 src, int32 dst, ulong next_idx = 0L, bool accepted = false)
        : msg_base(term, type, src, dst), next_idx_(next_idx), accepted_(accepted)
    {
    }

    __nocopy__(resp_msg);

public:
    ulong get_next_idx() const
    {
        return next_idx_;
    }

    bool get_accepted() const
    {
        return accepted_;
    }

    void accept(ulong next_idx)
    {
        next_idx_ = next_idx;
        accepted_ = true;
    }

private:
    ulong next_idx_;
    bool accepted_;
};
} // namespace cornerstone

#endif //_RESP_MSG_HXX_