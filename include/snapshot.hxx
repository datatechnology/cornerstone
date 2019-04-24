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

#ifndef _SNAPSHOT_HXX_
#define _SNAPSHOT_HXX_

namespace cornerstone {
    class snapshot {
    public:
        snapshot(ulong last_log_idx, ulong last_log_term, const ptr<cluster_config>& last_config, ulong size = 0)
            : last_log_idx_(last_log_idx), last_log_term_(last_log_term), size_(size), last_config_(last_config){}

        __nocopy__(snapshot)

    public:
        ulong get_last_log_idx() const {
            return last_log_idx_;
        }

        ulong get_last_log_term() const {
            return last_log_term_;
        }

        ulong size() const {
            return size_;
        }

        const ptr<cluster_config>& get_last_config() const {
            return last_config_;
        }

        static ptr<snapshot> deserialize(buffer& buf);

        bufptr serialize();

    private:
        ulong last_log_idx_;
        ulong last_log_term_;
        ulong size_;
        ptr<cluster_config> last_config_;
    };
}

#endif