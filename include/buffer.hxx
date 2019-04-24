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

#ifndef _BUFFER_HXX_
#define _BUFFER_HXX_

namespace cornerstone {

    class buffer;
    using bufptr = uptr<buffer, void(*)(buffer*)>;

    class buffer {
        buffer() = delete;
        __nocopy__(buffer)
    public:
        static bufptr alloc(const size_t size);
        static bufptr copy(const buffer& buf);
        
        size_t size() const;
        size_t pos() const;
        void pos(size_t p);

        int32 get_int();
        ulong get_ulong();
        byte get_byte();
        void get(bufptr& dst);
        const char* get_str();
        byte* data() const;

        void put(byte b);
        void put(int32 val);
        void put(ulong val);
        void put(const std::string& str);
        void put(const buffer& buf);
    };

    std::ostream& operator << (std::ostream& out, buffer& buf);
    std::istream& operator >> (std::istream& in, buffer& buf);
}
#endif //_BUFFER_HXX_