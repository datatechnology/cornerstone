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

#include "../include/cornerstone.hxx"

using namespace cornerstone;

ptr<snapshot_sync_req> snapshot_sync_req::deserialize(buffer& buf) {
    ptr<snapshot> snp(snapshot::deserialize(buf));
    ulong offset = buf.get_ulong();
    bool done = buf.get_byte() == 1;
    byte* src = buf.data();
    if (buf.pos() < buf.size()) {
        size_t sz = buf.size() - buf.pos();
        bufptr b = buffer::alloc(sz);
        ::memcpy(b->data(), src, sz);
        return cs_new<snapshot_sync_req>(snp, offset, std::move(b), done);
    }
    else {
        return cs_new<snapshot_sync_req>(snp, offset, buffer::alloc(0), done);
    }
}

bufptr snapshot_sync_req::serialize() {
    bufptr snp_buf = snapshot_->serialize();
    bufptr buf = buffer::alloc(snp_buf->size() + sz_ulong + sz_byte + (data_->size() - data_->pos()));
    buf->put(*snp_buf);
    buf->put(offset_);
    buf->put(done_ ? (byte)1 : (byte)0);
    buf->put(*data_);
    buf->pos(0);
    return buf;
}