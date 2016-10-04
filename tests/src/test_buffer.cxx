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

#include "../../include/cornerstone.hxx"
#include <sstream>
#include <cassert>

using namespace cornerstone;

static void do_test(ptr<buffer>& buf);

void test_buffer() {
    
    ptr<buffer> buf = buffer::alloc(1024);
    assert(buf->size() == 1024);
    do_test(buf);

    buf = buffer::alloc(0x8000);
    assert(buf->size() == 0x8000);
    do_test(buf);

    buf = buffer::alloc(0x10000);
    assert(buf->size() == 0x10000);
    do_test(buf);
}

static void do_test(ptr<buffer>& buf) {
    uint seed = (uint)std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<int32> distribution(1, 10000);
    auto rnd = std::bind(distribution, engine);

    // store int32 values into buffer
    std::vector<int32> vals;
    for (int i = 0; i < 100; ++i) {
        int32 val = rnd();
        vals.push_back(val);
        buf->put(val);
    }

    assert(buf->pos() == 100 * sz_int);

    ulong long_val = std::numeric_limits<uint>::max();
    long_val += rnd();
    buf->put(long_val);

    byte b = (byte)rnd();
    buf->put(b);
    buf->put("a string");
    byte b1 = (byte)rnd();
    buf->put(b1);
    ptr<buffer> buf1(buffer::alloc(100));
    buf1->put("another string");
    buf1->pos(0);
    ptr<buffer> buf2(buffer::copy(*buf1));
    buf->put(*buf1);
    buf->pos(0);
    ptr<buffer> buf3(buffer::alloc(sz_int * 100));
    buf->get(buf3);
    buf->pos(0);
    for (int i = 0; i < 100; ++i) {
        int32 val = buf->get_int();
        assert(val == vals[i]);
    }

    buf3->pos(0);
    for (int i = 0; i < 100; ++i) {
        int32 val = buf3->get_int();
        assert(val == vals[i]);
    }

    assert(long_val == buf->get_ulong());
    assert(b == buf->get_byte());
    assert(strcmp("a string", buf->get_str()) == 0);
    assert(b1 == buf->get_byte());
    assert(strcmp("another string", buf->get_str()) == 0);
    assert(strcmp("another string", buf2->get_str()) == 0);
    assert(buf->pos() == (100 * sz_int + 2 * sz_byte + sz_ulong + strlen("a string") + 1 + strlen("another string") + 1));

    std::stringstream stream;
    long_val = std::numeric_limits<uint>::max();
    long_val += rnd();
    ptr<buffer> lbuf(buffer::alloc(sizeof(ulong)));
    lbuf->put(long_val);
    lbuf->pos(0);
    stream << *lbuf;
    stream.seekp(0);
    ptr<buffer> lbuf1(buffer::alloc(sizeof(ulong)));
    stream >> *lbuf1;
    ulong long_val_copy = lbuf1->get_ulong();
    assert(long_val == long_val_copy);
}