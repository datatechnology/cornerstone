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

#ifndef _STRING_FORMATTER_HXX_
#define _STRING_FORMATTER_HXX_

#include <cstdio>
#include "pp_util.hxx"

namespace cornerstone
{
template <int N>
class strfmt
{
public:
    strfmt(const char* fmt) : fmt_(fmt)
    {
    }

    template <typename... TArgs>
    const char* fmt(TArgs... args)
    {
        ::snprintf(buf_, N, fmt_, args...);
        return buf_;
    }

    __nocopy__(strfmt) private : char buf_[N];
    const char* fmt_;
};

typedef strfmt<100> sstrfmt;
typedef strfmt<200> lstrfmt;
} // namespace cornerstone

#endif //_STRING_FORMATTER_HXX_