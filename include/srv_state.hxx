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

#ifndef _SRV_STATE_HXX_
#define _SRV_STATE_HXX_
#include <unordered_set>

namespace cornerstone {
    class srv_state {
    public:
        srv_state()
            : term_(0L), voted_for_(-1) {}

        __nocopy__(srv_state)

    public:
        ulong get_term() const { return term_; }
        void set_term(ulong term) { term_ = term; }
        int get_voted_for() const { return voted_for_; }
        void set_voted_for(int voted_for) { voted_for_ = voted_for; }
        void inc_term() { term_ += 1; }
    private:
        ulong term_;
        int voted_for_;
    };

    /**
     * a transient state between follower and candidate.
     * this state is not need to be persisted.
     */
    class prevote_state {
    public:
        prevote_state()
            : voted_servers_(), accepted_votes_(0){}
        
        __nocopy__(prevote_state)
    
    public:
        inline std::size_t num_of_votes() const {
            return voted_servers_.size();
        }

        inline int32 get_accepted_votes() const {
            return accepted_votes_;
        }

        inline void inc_accepted_votes() {
            accepted_votes_ ++;
        }

        inline bool add_voted_server(const int32 srv_id) {
            auto result = voted_servers_.emplace(srv_id);
            return result.second;
        }

        inline void reset() {
            voted_servers_.clear();
            accepted_votes_ = 0;
        }

        inline bool empty() const {
            return voted_servers_.empty();
        }

    private:
        std::unordered_set<int32> voted_servers_;
        int32 accepted_votes_;
    };
}

#endif
