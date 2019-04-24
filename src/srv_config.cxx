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

ptr<srv_config> srv_config::deserialize(buffer& buf) {
    int32 id = buf.get_int();
    const char* endpoint = buf.get_str();
    return cs_new<srv_config>(id, endpoint);
}

bufptr srv_config::serialize() const{
    bufptr buf = buffer::alloc(sz_int + endpoint_.length() + 1);
    buf->put(id_);
    buf->put(endpoint_);
    buf->pos(0);
    return buf;
}