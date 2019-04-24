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

#ifndef _SRV_CONFIG_HXX_
#define _SRV_CONFIG_HXX_

namespace cornerstone {
    class srv_config {
    public:
        srv_config(int32 id, const std::string& endpoint)
            : id_(id), endpoint_(endpoint) {}

        __nocopy__(srv_config)

    public:
        static ptr<srv_config> deserialize(buffer& buf);

        int32 get_id() const {
            return id_;
        }

        const std::string& get_endpoint() const {
            return endpoint_;
        }

        bufptr serialize() const;
    private:
        int32 id_;
        std::string endpoint_;
    };
}

#endif