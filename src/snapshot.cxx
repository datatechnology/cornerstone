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

#include "../include/cornerstone.hxx"

using namespace cornerstone;

ptr<snapshot> snapshot::deserialize(buffer& buf) {
    ulong last_log_idx = buf.get_ulong();
    ulong last_log_term = buf.get_ulong();
    ulong size = buf.get_ulong();
    ptr<cluster_config> conf(cluster_config::deserialize(buf));
    return cs_new<snapshot>(last_log_idx, last_log_term, conf, size);
}

bufptr snapshot::serialize() {
    bufptr conf_buf = last_config_->serialize();
    bufptr buf = buffer::alloc(conf_buf->size() + sz_ulong * 3);
    buf->put(last_log_idx_);
    buf->put(last_log_term_);
    buf->put(size_);
    buf->put(*conf_buf);
    buf->pos(0);
    return buf;
}