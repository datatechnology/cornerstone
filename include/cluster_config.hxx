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

#ifndef _CLUSTER_CONFIG_HXX_
#define _CLUSTER_CONFIG_HXX_

namespace cornerstone {
    class cluster_config {
    public:
        cluster_config(ulong log_idx = 0L, ulong prev_log_idx = 0L)
            : log_idx_(log_idx), prev_log_idx_(prev_log_idx), servers_() {}

        ~cluster_config() {
        }

        __nocopy__(cluster_config)
    public:
        typedef std::list<ptr<srv_config>>::iterator srv_itor;
        typedef std::list<ptr<srv_config>>::const_iterator const_srv_itor;

        static ptr<cluster_config> deserialize(buffer& buf);

        ulong get_log_idx() const {
            return log_idx_;
        }

        void set_log_idx(ulong log_idx) {
            prev_log_idx_ = log_idx_;
            log_idx_ = log_idx;
        }

        ulong get_prev_log_idx() const {
            return prev_log_idx_;
        }

        std::list<ptr<srv_config>>& get_servers() {
            return servers_;
        }

        ptr<srv_config>  get_server(int id) const {
            for (const_srv_itor it = servers_.begin(); it != servers_.end(); ++it) {
                if ((*it)->get_id() == id) {
                    return *it;
                }
            }

            return ptr<srv_config>();
        }

        bufptr serialize();
    private:
        ulong log_idx_;
        ulong prev_log_idx_;
        std::list<ptr<srv_config>> servers_;
    };
}

#endif //_CLUSTER_CONFIG_HXX_