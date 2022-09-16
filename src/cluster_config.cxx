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

#include "cluster_config.hxx"
#include <vector>

using namespace cornerstone;

bufptr cluster_config::serialize()
{
    size_t sz = 2 * sz_ulong + sz_int;
    std::vector<bufptr> srv_buffs;
    for (cluster_config::const_srv_itor it = servers_.begin(); it != servers_.end(); ++it)
    {
        bufptr buf = (*it)->serialize();
        sz += buf->size();
        srv_buffs.emplace_back(std::move(buf));
    }

    bufptr result = buffer::alloc(sz);
    result->put(log_idx_);
    result->put(prev_log_idx_);
    result->put((int32)servers_.size());
    for (auto& item : srv_buffs)
    {
        result->put(*item);
    }

    result->pos(0);
    return result;
}

ptr<cluster_config> cluster_config::deserialize(buffer& buf)
{
    ulong log_idx = buf.get_ulong();
    ulong prev_log_idx = buf.get_ulong();
    int32 cnt = buf.get_int();
    ptr<cluster_config> conf = cs_new<cluster_config>(log_idx, prev_log_idx);
    while (cnt-- > 0)
    {
        conf->get_servers().push_back(srv_config::deserialize(buf));
    }

    return conf;
}