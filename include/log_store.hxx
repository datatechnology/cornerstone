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

#ifndef _LOG_STORE_HXX_
#define _LOG_STORE_HXX_

namespace cornerstone {
    class log_store {
    __interface_body__(log_store)

    public:
        /**
        ** The first available slot of the store, starts with 1
        */
        virtual ulong next_slot() const = 0;

        /**
        ** The start index of the log store, at the very beginning, it must be 1
        ** however, after some compact actions, this could be anything greater or equals to one
        */
        virtual ulong start_index() const = 0;

        /**
        * The last log entry in store
        * @return a dummy constant entry with value set to null and term set to zero if no log entry in store
        */
        virtual ptr<log_entry> last_entry() const = 0;

        /**
        * Appends a log entry to store
        * @param entry
        */
        virtual ulong append(ptr<log_entry>& entry) = 0;

        /**
        * Over writes a log entry at index of {@code index}
        * @param index a value < this->next_slot(), and starts from 1
        * @param entry
        */
        virtual void write_at(ulong index, ptr<log_entry>& entry) = 0;

        /**
        * Get log entries with index between start and end
        * @param start, the start index of log entries
        * @param end, the end index of log entries (exclusive)
        * @return the log entries between [start, end), nilptr is returned if no entries in that range
        */
        virtual ptr<std::vector<ptr<log_entry>>> log_entries(ulong start, ulong end) = 0;

        /**
        * Gets the log entry at the specified index
        * @param index, starts from 1
        * @return the log entry or null if index >= this->next_slot()
        */
        virtual ptr<log_entry> entry_at(ulong index) = 0;

        /**
        * Gets the term for the log entry at the specified index
        * Suggest to stop the system if the index >= this->next_slot()
        * @param index, starts from 1
        * @return the term for the specified log entry or 0 if index < this->start_index()
        */
        virtual ulong term_at(ulong index) = 0;

        /**
        * Pack cnt log items starts from index
        * @param index
        * @param cnt
        * @return log pack
        */
        virtual bufptr pack(ulong index, int32 cnt) = 0;

        /**
        * Apply the log pack to current log store, starting from index
        * @param index, the log index that start applying the pack, index starts from 1
        * @param pack
        */
        virtual void apply_pack(ulong index, buffer& pack) = 0;

        /**
        * Compact the log store by removing all log entries including the log at the last_log_index
        * @param last_log_index
        * @return compact successfully or not
        */
        virtual bool compact(ulong last_log_index) = 0;
    };
}

#endif //_LOG_STORE_HXX_
