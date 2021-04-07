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

#define __is_big_block(p) (0x80000000 & *((uint*)(p)))
#define __init_block(p, s, t) ((t*)(p))[0] = (t)s;\
    ((t*)(p))[1] = 0
#define __init_s_block(p, s) __init_block(p, s, ushort)
#define __init_b_block(p, s) __init_block(p, s, uint);\
    *((uint*)(p)) |= 0x80000000
#define __pos_of_s_block(p) ((ushort*)(p))[1]
#define __pos_of_b_block(p) ((uint*)(p))[1]
#define __size_of_block(p) (__is_big_block(p)) ? (*((uint*)(p)) ^ 0x80000000) : *((ushort*)(p))
#define __pos_of_block(p) (__is_big_block(p)) ? __pos_of_b_block(p) : __pos_of_s_block(p)
#define __mv_fw_block(p, d) if(__is_big_block(p)){\
    ((uint*)(p))[1] += (d);\
    }\
    else{\
    ((ushort*)(p))[1] += (ushort)(d);\
    }
#define __set_block_pos(p, pos) if(__is_big_block(p)){\
    ((uint*)(p))[1] = (pos);\
    }\
    else{\
    ((ushort*)(p))[1] = (ushort)(pos);\
    }
#define __data_of_block(p) (__is_big_block(p)) ? (byte*) (((byte*)(((uint*)(p)) + 2)) + __pos_of_b_block(p)) : (byte*) (((byte*)(((ushort*)p) + 2)) + __pos_of_s_block(p))
using namespace cornerstone;

static void free_buffer(buffer* buf) {
    if (buf != nullptr) {
        delete[] reinterpret_cast<char*>(buf);
    }
}

bufptr buffer::alloc(const size_t size) {
    if (size >= 0x80000000) {
        throw std::out_of_range("size exceed the max size that cornrestone::buffer could support");
    }

    if (size >= 0x8000) {
        bufptr buf(reinterpret_cast<buffer*>(new char[size + sizeof(uint) * 2]), &free_buffer);
        any_ptr ptr = reinterpret_cast<any_ptr>(buf.get());
        __init_b_block(ptr, size);
        return buf;
    }

    bufptr buf(reinterpret_cast<buffer*>(new char[size + sizeof(ushort) * 2]), &free_buffer);
    any_ptr ptr = reinterpret_cast<any_ptr>(buf.get());
    __init_s_block(ptr, size);
    return buf;
}

bufptr buffer::copy(const buffer& buf) {
    bufptr other = alloc(buf.size() - buf.pos());
    other->put(buf);
    other->pos(0);
    return other;
}

size_t buffer::size() const {
    return (size_t)(__size_of_block(this));
}

size_t buffer::pos() const {
    return (size_t)(__pos_of_block(this));
}

byte* buffer::data() const {
    return __data_of_block(this);
}

int32 buffer::get_int() {
    size_t avail = size() - pos();
    if (avail < sz_int) {
        throw std::overflow_error("insufficient buffer available for an int32 value");
    }

    byte* d = data();
    int32 val = 0;
    for (size_t i = 0; i < sz_int; ++i) {
        int32 byte_val = (int32)*(d + i);
        val += (byte_val << (i * 8));
    }

    __mv_fw_block(this, sz_int);
    return val;
}

ulong buffer::get_ulong() {
    size_t avail = size() - pos();
    if (avail < sz_ulong) {
        throw std::overflow_error("insufficient buffer available for an ulong value");
    }

    byte* d = data();
    ulong val = 0L;
    for (size_t i = 0; i < sz_ulong; ++i) {
        ulong byte_val = (ulong)*(d + i);
        val += (byte_val << (i * 8));
    }

    __mv_fw_block(this, sz_ulong);
    return val;
}

byte buffer::get_byte() {
    size_t avail = size() - pos();
    if (avail < sz_byte) {
        throw std::overflow_error("insufficient buffer available for a byte");
    }

    byte val = *data();
    __mv_fw_block(this, sz_byte);
    return val;
}

void buffer::pos(size_t p) {
    size_t position = p > size() ? size() : p;
    __set_block_pos(this, position);
}

const char* buffer::get_str() {
    size_t p = pos();
    size_t s = size();
    size_t i = 0;
    byte* d = data();
    while ((p + i) < s && *(d + i)) ++i;
    if (p + i >= s || i == 0) {
        return nilptr;
    }

    __mv_fw_block(this, i + 1);
    return reinterpret_cast<const char*>(d);
}

void buffer::put(byte b) {
    if (size() - pos() < sz_byte) {
        throw std::overflow_error("insufficient buffer to store byte");
    }

    byte* d = data();
    *d = b;
    __mv_fw_block(this, sz_byte);
}

void buffer::put(int32 val) {
    if (size() - pos() < sz_int) {
        throw std::overflow_error("insufficient buffer to store int32");
    }

    byte* d = data();
    for (size_t i = 0; i < sz_int; ++i) {
        *(d + i) = (byte)(val >> (i * 8));
    }

    __mv_fw_block(this, sz_int);
}

void buffer::put(ulong val) {
    if (size() - pos() < sz_ulong) {
        throw std::overflow_error("insufficient buffer to store int32");
    }

    byte* d = data();
    for (size_t i = 0; i < sz_ulong; ++i) {
        *(d + i) = (byte)(val >> (i * 8));
    }

    __mv_fw_block(this, sz_ulong);
}

void buffer::put(const std::string& str) {
    if (size() - pos() < (str.length() + 1)) {
        throw std::overflow_error("insufficient buffer to store a string");
    }

    byte* d = data();
    for (size_t i = 0; i < str.length(); ++i) {
        *(d + i) = (byte)str[i];
    }

    *(d + str.length()) = (byte)0;
    __mv_fw_block(this, str.length() + 1);
}

void buffer::get(bufptr& dst) {
    size_t sz = dst->size() - dst->pos();
    ::memcpy(dst->data(), data(), sz);
    __mv_fw_block(this, sz);
}

void buffer::put(const buffer& buf) {
    size_t sz = size();
    size_t p = pos();
    size_t src_sz = buf.size();
    size_t src_p = buf.pos();
    if ((sz - p) < (src_sz - src_p)) {
        throw std::overflow_error("insufficient buffer to hold the other buffer");
    }

    byte* d = data();
    byte* src = buf.data();
    ::memcpy(d, src, src_sz - src_p);
    __mv_fw_block(this, src_sz - src_p);
}

std::ostream& cornerstone::operator << (std::ostream& out, buffer& buf) {
    if (!out) {
        throw std::ios::failure("bad output stream.");
    }

    out.write(reinterpret_cast<char*>(buf.data()), buf.size() - buf.pos());

    if (!out) {
        throw std::ios::failure("write failed");
    }

    return out;
}

std::istream& cornerstone::operator >> (std::istream& in, buffer& buf) {
    if (!in) {
        throw std::ios::failure("bad input stream");
    }

    char* data = reinterpret_cast<char*>(buf.data());
    int size = buf.size() - buf.pos();
    in.read(data, size);

    if (!in) {
        throw std::ios::failure("read failed");
    }

    return in;
}