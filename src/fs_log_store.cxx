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

#define LOG_INDEX_FILE "store.idx"
#define LOG_DATA_FILE "store.dat"
#define LOG_START_INDEX_FILE "store.sti"
#define LOG_INDEX_FILE_BAK "store.idx.bak"
#define LOG_DATA_FILE_BAK "store.dat.bak"
#define LOG_START_INDEX_FILE_BAK "store.sti.bak"

#ifdef _WIN32
#include <Windows.h>
#define PATH_SEPARATOR '\\'
int truncate(const char* path, ulong new_size) {
    HANDLE file_handle = ::CreateFileA(path, GENERIC_WRITE, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (file_handle == INVALID_HANDLE_VALUE) {
        return -1;
    }

    LONG offset = new_size & 0xFFFFFFFF;
    LONG offset_high = (LONG)(new_size >> 32);
    LONG result = ::SetFilePointer(file_handle, offset, &offset_high, FILE_BEGIN);
    if (result == INVALID_SET_FILE_POINTER) {
        CloseHandle(file_handle);
        return -1;
    }

    if (!::SetEndOfFile(file_handle)) {
        CloseHandle(file_handle);
        return -1;
    }

    CloseHandle(file_handle);
    return 0;
}
#undef max
#undef min
#else
// for truncate function
#include <unistd.h>
#include <sys/types.h>
#define PATH_SEPARATOR '/'
#ifdef OS_FREEBSD
int truncate(const char* path, ulong new_size) {
    return ::truncate(path, (off_t)new_size);
}
#else
int truncate(const char* path, ulong new_size) {
    return ::truncate64(path, (off64_t)new_size);
}
#endif
#endif

using namespace cornerstone;
ptr<log_entry> empty_entry(cs_new<log_entry>(0, buffer::alloc(0), log_val_type::app_log));


ptr<log_entry> log_store_buffer::operator[](ulong idx) {
    read_lock(lock_);
    size_t idx_within_buf = static_cast<size_t>(idx - start_idx_);
    if (idx_within_buf >= buf_.size() || idx < start_idx_) {
        return ptr<log_entry>();
    }

    return buf_[idx_within_buf];
}

// [start, end), returns the start_idx_;
ulong log_store_buffer::fill(ulong start, ulong end, std::vector<ptr<log_entry>>& result) {
    read_lock(lock_);
    if (end < start_idx_) {
        return start_idx_;
    }

    int offset = static_cast<int>(start - start_idx_);
    if (offset > 0) {
        for (int i = 0; i < static_cast<int>(end - start); ++i) {
            result.emplace_back(buf_[offset + i]);
        }
    }
    else {
        offset *= -1;
        for (int i = 0; i < offset; ++i) {
            result.emplace_back(ptr<log_entry>()); // make room for items that doesn't found in the buffer
        }

        for (int i = 0; i < static_cast<int>(end - start_idx_); ++i) {
            result.emplace_back(buf_[i]);
        }
    }

    return start_idx_;
}

// trimming the buffer [start, end)
void log_store_buffer::trim(ulong start) {
    write_lock(lock_);
    if (start < start_idx_) {
        return;
    }

    size_t index = static_cast<size_t>(start - start_idx_);
    if (index < buf_.size()) {
        buf_.erase(buf_.begin() + index, buf_.end());
    }
}

void log_store_buffer::append(ptr<log_entry>& entry) {
    write_lock(lock_);
    buf_.emplace_back(entry);
    if ((size_t)max_size_ < buf_.size()) {
        buf_.erase(buf_.begin());
        start_idx_ += 1;
    }
}


fs_log_store::~fs_log_store() {
    recur_lock(store_lock_);
    if (idx_file_) {
        idx_file_.close();
    }

    if (data_file_) {
        data_file_.close();
    }

    if (start_idx_file_) {
        start_idx_file_.close();
    }
}

fs_log_store::fs_log_store(const std::string& log_folder, int buf_size)
    : idx_file_(), 
    data_file_(), 
    start_idx_file_(), 
    entries_in_store_(0), 
    start_idx_(1), 
    log_folder_(log_folder), 
    store_lock_(), 
    buf_(nilptr), 
    buf_size_(buf_size < 0 ? std::numeric_limits<int>::max() : buf_size) {
    if (log_folder_.length() > 0 && log_folder_[log_folder_.length() - 1] != PATH_SEPARATOR) {
        log_folder_.push_back(PATH_SEPARATOR);
    }

    idx_file_.open(log_folder_ + LOG_INDEX_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    if (!idx_file_) {
        // create the file and reopen
        idx_file_.open(log_folder_ + LOG_INDEX_FILE, std::fstream::out);
        idx_file_.close();
        idx_file_.open(log_folder_ + LOG_INDEX_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    data_file_.open(log_folder_ + LOG_DATA_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    if (!data_file_) {
        // create the file and reopen
        data_file_.open(log_folder_ + LOG_DATA_FILE, std::fstream::out);
        data_file_.close();
        data_file_.open(log_folder_ + LOG_DATA_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    start_idx_file_.open(log_folder_ + LOG_START_INDEX_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    if (!start_idx_file_) {
        start_idx_file_.open(log_folder_ + LOG_START_INDEX_FILE, std::fstream::out);
        start_idx_file_.close();
        start_idx_file_.open(log_folder_ + LOG_START_INDEX_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    if (!idx_file_ || !data_file_ || !start_idx_file_) {
        throw std::runtime_error("fail to create store files");
    }

    if (start_idx_file_.tellp() > 0) {
        start_idx_file_.seekg(0, std::fstream::beg);
        bufptr idx_buf(buffer::alloc(sz_ulong));
        start_idx_file_ >> *idx_buf;
        start_idx_ = idx_buf->get_ulong();
        entries_in_store_ = idx_file_.tellp() / sz_ulong;
    }
    else {
        start_idx_ = 1;
        bufptr idx_buf(buffer::alloc(sz_ulong));
        idx_buf->put(start_idx_);
        idx_buf->pos(0);
        start_idx_file_.seekp(0, std::fstream::beg);
        start_idx_file_ << *idx_buf;
        entries_in_store_ = 0;
        start_idx_file_.flush();
    }

    buf_ = std::make_unique<log_store_buffer>(entries_in_store_ > (size_t)buf_size_ ? entries_in_store_ - buf_size_ + start_idx_ : start_idx_, buf_size_);
    fill_buffer();
}

ulong fs_log_store::next_slot() const {
    recur_lock(store_lock_);
    return start_idx_ + entries_in_store_;
}

ulong fs_log_store::start_index() const {
    recur_lock(store_lock_);
    return start_idx_;
}

ptr<log_entry> fs_log_store::last_entry() const {
    ptr<log_entry> entry = buf_->last_entry();
    if (entry == nilptr) {
        return empty_entry;
    }

    return entry;
}

ulong fs_log_store::append(ptr<log_entry>& entry) {
    recur_lock(store_lock_);
    idx_file_.seekp(0, std::fstream::end);
    data_file_.seekp(0, std::fstream::end);
    bufptr idx_buf(buffer::alloc(sz_ulong));
    idx_buf->put(static_cast<ulong>(data_file_.tellp()));
    idx_buf->pos(0);
    idx_file_ << *idx_buf;
    bufptr entry_buf = entry->serialize();
    data_file_ << *entry_buf;
    buf_->append(entry);
    entries_in_store_ += 1;
    idx_file_.flush();
    data_file_.flush();
    if (!data_file_ || !idx_file_) {
        throw std::runtime_error("IO fails, data cannot be saved");
    }

    return start_idx_ + entries_in_store_ - 1;
}

void fs_log_store::write_at(ulong index, ptr<log_entry>& entry) {
    recur_lock(store_lock_);
    if (index < start_idx_ || index > start_idx_ + entries_in_store_) {
        throw std::range_error("index out of range");
    }

    ulong local_idx = index - start_idx_ + 1; //start_idx is one based
    ulong idx_pos = (local_idx - 1) * sz_ulong;
    if (local_idx <= entries_in_store_) {
        idx_file_.seekg(idx_pos, std::fstream::beg);
        bufptr buf(buffer::alloc(sz_ulong));
        idx_file_ >> *buf;
        data_file_.seekp(buf->get_ulong(), std::fstream::beg);
    }
    else {
        data_file_.seekp(0, std::fstream::end);
    }

    idx_file_.seekp(idx_pos, std::fstream::beg);
    ulong data_pos = data_file_.tellp();
    bufptr ibuf(buffer::alloc(sz_ulong));
    ibuf->put(data_pos);
    ibuf->pos(0);
    idx_file_ << *ibuf;
    bufptr ebuf = entry->serialize();
    data_file_ << *ebuf;
    idx_file_.flush();
    data_file_.flush();

    // truncate the files if necessary
    ulong ndata_len = data_file_.tellp();
    ulong nidx_len = idx_file_.tellp();
    idx_file_.seekp(0, std::fstream::end);
    if (static_cast<ulong>(idx_file_.tellp()) > nidx_len) { // new index length is less than current file size, truncate it
        std::string idx_path = log_folder_ + LOG_INDEX_FILE;
        std::string data_path = log_folder_ + LOG_DATA_FILE;
        idx_file_.close();
        data_file_.close();
        if (truncate(idx_path.c_str(), nidx_len) < 0) {
            throw std::ios_base::failure("failed to truncate the index file");
        }

        if (truncate(data_path.c_str(), ndata_len) < 0) {
            throw std::ios_base::failure("failed to truncate the data file");
        }

        idx_file_.open(idx_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
        data_file_.open(data_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    if (local_idx <= entries_in_store_) {
        buf_->trim(index);
    }

    buf_->append(entry);
    entries_in_store_ = local_idx;
    if (!data_file_ || !idx_file_) {
        throw std::runtime_error("IO fails, data cannot be saved");
    }
}

ptr<std::vector<ptr<log_entry>>> fs_log_store::log_entries(ulong start, ulong end) {
    ulong lstart(0), lend(0), good_end(0);
    {
        recur_lock(store_lock_);
        if (start < start_idx_) {
            throw std::range_error("index out of range");
        }

        if (start >= end || start >= (start_idx_ + entries_in_store_)) {
            return ptr<std::vector<ptr<log_entry>>>();
        }

        lstart = start - start_idx_;
        lend = end - start_idx_;
        lend = lend > entries_in_store_ ? entries_in_store_ : lend;
        good_end = lend + start_idx_;
    }

    if (lstart >= lend) {
        return ptr<std::vector<ptr<log_entry>>>();
    }

    ptr<std::vector<ptr<log_entry>>> results(cs_new<std::vector<ptr<log_entry>>>());

    // fill with the buffer
    ulong buffer_first_idx = buf_->fill(start, good_end, *results);

    // Assumption: buffer.last_index() == entries_in_store_ + start_idx_
    // (Yes, for sure, we need to enforce this assumption to be true)
    if (start < buffer_first_idx) {
        // in this case, we need to read from store file
        recur_lock(store_lock_);
        if (!data_file_ || !idx_file_) {
            throw std::runtime_error("IO fails, data cannot be saved");
        }

        lend = buffer_first_idx - start_idx_;
        idx_file_.seekg(lstart * sz_ulong);
        bufptr d_start_idx_buf(buffer::alloc(sz_ulong));
        idx_file_ >> *d_start_idx_buf;
        ulong data_start = d_start_idx_buf->get_ulong();
        for (int i = 0; i < (int)(lend - lstart); ++i) {
            bufptr d_end_idx_buf(buffer::alloc(sz_ulong));
            idx_file_ >> *d_end_idx_buf;
            ulong data_end = d_end_idx_buf->get_ulong();
            int data_sz = (int)(data_end - data_start);
            data_file_.seekg(data_start);
            bufptr entry_buf(buffer::alloc(data_sz));
            data_file_ >> *entry_buf;
            if (results->size() > static_cast<size_t>(i)) {
                (*results)[i] = log_entry::deserialize(*entry_buf);
            } else {
                results->emplace_back(std::move(log_entry::deserialize(*entry_buf)));
            }
            
            data_start = data_end;
        }
    }

    return results;
}

ptr<log_entry> fs_log_store::entry_at(ulong index) {

    ptr<log_entry> entry = (*buf_)[index];
    if (entry) {
        return entry;
    }

    {
        // since we don't hit the buffer, so this must not be the last entry 
        // (according to Assumption: buffer.last_index() == entries_in_store_ + start_idx_)
        recur_lock(store_lock_);
        if (index < start_idx_) {
            throw std::range_error("index out of range");
        }

        if (!data_file_ || !idx_file_) {
            throw std::runtime_error("IO fails, data cannot be saved");
        }

        if (index >= start_idx_ + entries_in_store_) {
            return ptr<log_entry>();
        }

        ulong idx_pos = (index - start_idx_) * sz_ulong;
        idx_file_.seekg(idx_pos);
        bufptr d_start_idx_buf(buffer::alloc(sz_ulong));
        idx_file_ >> *d_start_idx_buf;
        ulong d_pos = d_start_idx_buf->get_ulong();
        bufptr d_end_idx_buf(buffer::alloc(sz_ulong));
        idx_file_ >> *d_end_idx_buf;
        ulong end_d_pos = d_end_idx_buf->get_ulong();
        data_file_.seekg(d_pos);
        bufptr entry_buf(buffer::alloc((int)(end_d_pos - d_pos)));
        data_file_ >> *entry_buf;
        return log_entry::deserialize(*entry_buf);
    }
}

ulong fs_log_store::term_at(ulong index) {
    if (index >= buf_->first_idx() && index < buf_->last_idx()) {
        return buf_->get_term(index);
    }

    {
        recur_lock(store_lock_);
        if (index < start_idx_) {
            throw std::range_error("index out of range");
        }

        if (!data_file_ || !idx_file_) {
            throw std::runtime_error("IO fails, data cannot be saved");
        }

        idx_file_.seekg(static_cast<int>(index - start_idx_) * sz_ulong);
        bufptr d_start_idx_buf(buffer::alloc(sz_ulong));
        idx_file_ >> *d_start_idx_buf;
        data_file_.seekg(d_start_idx_buf->get_ulong());

        // IMPORTANT!! 
        // We hack the log_entry serialization details here
        bufptr term_buf(buffer::alloc(sz_ulong));
        data_file_ >> *term_buf;
        return term_buf->get_ulong();
    }
}

bufptr fs_log_store::pack(ulong index, int32 cnt) {
    recur_lock(store_lock_);
    if (index < start_idx_) {
        throw std::range_error("index out of range");
    }

    if (!data_file_ || !idx_file_) {
        throw std::runtime_error("IO fails, data cannot be saved");
    }

    ulong offset = index - start_idx_;
    if (offset >= entries_in_store_) {
        return buffer::alloc(0);
    }

    ulong end_offset = std::min(offset + cnt, entries_in_store_);
    idx_file_.seekg(static_cast<int>(offset) * sz_ulong);
    bool read_to_end = end_offset == entries_in_store_;
    bufptr idx_buf(buffer::alloc(static_cast<int>(end_offset - offset) * sz_ulong));
    idx_file_ >> *idx_buf;
    ulong end_of_data(0);
    if (read_to_end) {
        data_file_.seekg(0, std::fstream::end);
        end_of_data = data_file_.tellg();
    }
    else {
        bufptr end_d_idx_buf(buffer::alloc(sz_ulong));
        idx_file_ >> *end_d_idx_buf;
        end_of_data = end_d_idx_buf->get_ulong();
    }

    idx_buf->pos(0);
    ulong start_of_data = idx_buf->get_ulong();
    idx_buf->pos(0);
    data_file_.seekg(start_of_data);
    bufptr data_buf(buffer::alloc(static_cast<int>(end_of_data - start_of_data)));
    data_file_ >> *data_buf;
    bufptr result = buffer::alloc(2 * sz_int + idx_buf->size() + data_buf->size());
    result->put(static_cast<int32>(idx_buf->size()));
    result->put(static_cast<int32>(data_buf->size()));
    result->put(*idx_buf);
    result->put(*data_buf);
    result->pos(0);
    return result;
}

void fs_log_store::apply_pack(ulong index, buffer& pack) {
    recur_lock(store_lock_);
    int32 idx_len = pack.get_int();
    int32 data_len = pack.get_int();
    ulong local_idx = index - start_idx_;
    if (local_idx == entries_in_store_) {
        idx_file_.seekp(0, std::fstream::end);
        data_file_.seekp(0, std::fstream::end);
    }
    else {
        ulong idx_pos = local_idx * sz_ulong;
        idx_file_.seekg(idx_pos);
        bufptr data_pos_buf(buffer::alloc(sz_ulong));
        idx_file_ >> *data_pos_buf;
        idx_file_.seekp(idx_pos);
        data_file_.seekp(data_pos_buf->get_ulong());
    }

    ulong prev_data_pos = data_file_.tellp();
    size_t prev_pack_pos = pack.pos();
    ulong first_idx = pack.get_ulong();
    pack.pos(prev_pack_pos);
    if (prev_data_pos == first_idx) {
        idx_file_.write(reinterpret_cast<const char*>(pack.data()), idx_len);
    } else {
        int32 byte_read = 0;
        while (byte_read < idx_len) {
            ulong adjusted_idx = pack.get_ulong() + prev_data_pos - first_idx;
            idx_file_.write(reinterpret_cast<const char*>(&adjusted_idx), sz_ulong);
            byte_read += sz_ulong;
        }
        pack.pos(prev_pack_pos);
    }
    data_file_.write(reinterpret_cast<const char*>(pack.data() + idx_len), data_len);
    idx_file_.flush();
    data_file_.flush();
    ulong idx_pos = idx_file_.tellp();
    ulong data_pos = data_file_.tellp();
    idx_file_.seekp(0, std::fstream::end);
    data_file_.seekp(0, std::fstream::end);
    if (idx_pos < static_cast<ulong>(idx_file_.tellp())) {
        std::string idx_path = log_folder_ + LOG_INDEX_FILE;
        std::string data_path = log_folder_ + LOG_DATA_FILE;
        idx_file_.close();
        data_file_.close();
        if (truncate(idx_path.c_str(), idx_pos) < 0) {
            throw std::ios_base::failure("failed to truncate the index file");
        }

        if (truncate(data_path.c_str(), data_pos) < 0) {
            throw std::ios_base::failure("failed to truncate the data file");
        }

        idx_file_.open(idx_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
        data_file_.open(data_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    if (!data_file_ || !idx_file_) {
        throw std::runtime_error("IO fails, data cannot be saved");
    }

    entries_in_store_ = local_idx + idx_len / sz_ulong;
    buf_->reset(entries_in_store_ > (size_t)buf_size_ ? entries_in_store_ + start_idx_ - buf_size_ : start_idx_);
    fill_buffer();
}

bool fs_log_store::compact(ulong last_log_index) {
    recur_lock(store_lock_);
    if (last_log_index < start_idx_) {
        throw std::range_error("index out of range");
    }

    // backup the files
    idx_file_.seekg(0);
    data_file_.seekg(0);
    start_idx_file_.seekg(0);
    std::string idx_bak_path = log_folder_ + LOG_INDEX_FILE_BAK;
    std::string data_bak_path = log_folder_ + LOG_DATA_FILE_BAK;
    std::string start_idx_bak_path = log_folder_ + LOG_START_INDEX_FILE_BAK;
    std::string data_file_path = log_folder_ + LOG_DATA_FILE;
    std::string idx_file_path = log_folder_ + LOG_INDEX_FILE;
    std::string start_idx_file_path = log_folder_ + LOG_START_INDEX_FILE;
    std::remove(idx_bak_path.c_str());
    std::remove(data_bak_path.c_str());
    std::remove(start_idx_bak_path.c_str());

    std::fstream idx_bak_file, data_bak_file, start_idx_bak_file;
    idx_bak_file.open(idx_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    if (!idx_bak_file) {
        idx_bak_file.open(idx_bak_path, std::fstream::out);
        idx_bak_file.close();
        idx_bak_file.open(idx_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    }

    data_bak_file.open(data_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    if (!data_bak_file) {
        data_bak_file.open(data_bak_path, std::fstream::out);
        data_bak_file.close();
        data_bak_file.open(data_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    }

    start_idx_bak_file.open(start_idx_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    if (!start_idx_bak_file) {
        start_idx_bak_file.open(start_idx_bak_path, std::fstream::out);
        start_idx_bak_file.close();
        start_idx_bak_file.open(start_idx_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    }

    if (!idx_bak_file || !data_bak_file || !start_idx_bak_file) return false; //we cannot proceed as backup files are bad

    idx_file_.seekg(0);
    data_file_.seekg(0);
    start_idx_file_.seekg(0);
    idx_bak_file << idx_file_.rdbuf();
    data_bak_file << data_file_.rdbuf();
    start_idx_bak_file << start_idx_file_.rdbuf();
    idx_bak_file.flush();
    data_bak_file.flush();
    start_idx_bak_file.flush();

    do {
        if (last_log_index >= start_idx_ + entries_in_store_ - 1) {
            // need to clear all entries in this store and update the start index
            idx_file_.close();
            data_file_.close();
            if (std::remove(idx_file_path.c_str()) != 0) break;
            if (std::remove(data_file_path.c_str()) != 0) break;
            idx_file_.open(idx_file_path, std::fstream::out);
            idx_file_.close();
            idx_file_.open(idx_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
            data_file_.open(data_file_path, std::fstream::out);
            data_file_.close();
            data_file_.open(data_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
            if (!idx_file_ || !data_file_) break;

            // save the logstore state
            start_idx_file_.seekp(0);
            bufptr start_idx_buf(buffer::alloc(sz_ulong));
            start_idx_buf->put(last_log_index + 1);
            start_idx_buf->pos(0);
            start_idx_file_ << *start_idx_buf;
            start_idx_file_.flush();
            start_idx_ = last_log_index + 1;
            entries_in_store_ = 0;
            buf_->reset(start_idx_);

            // close all backup files
            idx_bak_file.close();
            data_bak_file.close();
            start_idx_bak_file.close();
            return true;
        }

        // else, we need to compact partial of the logs
        ulong local_last_idx = last_log_index - start_idx_;
        ulong idx_pos = (local_last_idx + 1) * sz_ulong;
        bufptr data_pos_buf(buffer::alloc(sz_ulong));
        idx_file_.seekg(idx_pos);
        idx_file_ >> *data_pos_buf;
        idx_file_.seekp(0, std::fstream::end);
        data_file_.seekp(0, std::fstream::end);
        ulong data_pos = data_pos_buf->get_ulong();
        ulong idx_len = static_cast<ulong>(idx_file_.tellp()) - idx_pos;
        ulong data_len = static_cast<ulong>(data_file_.tellp()) - data_pos;

        // compact the data file
        data_bak_file.seekg(data_pos);
        data_file_.seekp(0);
        data_file_ << data_bak_file.rdbuf();

        // truncate the data file
        data_file_.close();
        if(0 != truncate(data_file_path.c_str(), data_len)) break;
        data_file_.open(data_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
        if (!data_file_) break;

        // compact the index file
        idx_bak_file.seekg(idx_pos);
        idx_file_.seekp(0);
        for (ulong i = 0; i < idx_len / sz_ulong; ++i) {
            data_pos_buf->pos(0);
            idx_bak_file >> *data_pos_buf;
            ulong new_pos = data_pos_buf->get_ulong() - data_pos;
            data_pos_buf->pos(0);
            data_pos_buf->put(new_pos);
            data_pos_buf->pos(0);
            idx_file_ << *data_pos_buf;
        }

        // truncate the index file
        idx_file_.close();
        if(0 != truncate(idx_file_path.c_str(), idx_len)) break;
        idx_file_.open(idx_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
        if (!idx_file_) break;

        // close all backup files
        idx_bak_file.close();
        data_bak_file.close();
        start_idx_bak_file.close();

        start_idx_file_.seekp(0);
        bufptr start_idx_buf(buffer::alloc(sz_ulong));
        start_idx_buf->put(last_log_index + 1);
        start_idx_buf->pos(0);
        start_idx_file_ << *start_idx_buf;
        start_idx_file_.flush();
        entries_in_store_ -= (last_log_index - start_idx_ + 1);
        start_idx_ = last_log_index + 1;
        buf_->reset(entries_in_store_ > (size_t)buf_size_ ? entries_in_store_ + start_idx_ - buf_size_ : start_idx_);
        fill_buffer();
        return true;
    } while (false);

    // restore the state due to errors
    if (idx_file_) idx_file_.close();
    if (data_file_) data_file_.close();
    if (start_idx_file_) start_idx_file_.close();
    std::remove(idx_file_path.c_str());
    std::remove(data_file_path.c_str());
    std::remove(start_idx_file_path.c_str());
    idx_bak_file.seekg(0);
    data_bak_file.seekg(0);
    start_idx_bak_file.seekg(0);
    data_file_.open(data_bak_path, std::fstream::out);
    data_file_.close();
    data_file_.open(data_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    idx_file_.open(idx_file_path, std::fstream::out);
    idx_file_.close();
    idx_file_.open(idx_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    start_idx_file_.open(start_idx_file_path, std::fstream::out);
    start_idx_file_.close();
    start_idx_file_.open(start_idx_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    if (!idx_file_ || !data_file_ || !start_idx_file_) {
        throw std::runtime_error("IO error, fails to restore files back.");
    }

    data_file_ << data_bak_file.rdbuf();
    idx_file_ << idx_bak_file.rdbuf();
    start_idx_file_ << start_idx_bak_file.rdbuf();
    data_file_.flush();
    idx_file_.flush();
    start_idx_file_.flush();

    // close all backup files
    idx_bak_file.close();
    data_bak_file.close();
    start_idx_bak_file.close();
    return false;
}

void fs_log_store::close() {
    idx_file_.close();
    data_file_.close();
    start_idx_file_.close();
}

void fs_log_store::fill_buffer() {
    ulong first_idx = buf_->first_idx();
    idx_file_.seekg(0, std::fstream::end);
    data_file_.seekg(0, std::fstream::end);
    if (idx_file_.tellg() > 0) {
        ulong idx_file_len = idx_file_.tellg();
        ulong data_file_len = data_file_.tellg();
        ulong idx_pos = (first_idx - start_idx_) * sz_ulong;
        idx_file_.seekg(idx_pos);
        bufptr idx_buf(buffer::alloc(static_cast<size_t>(idx_file_len - idx_pos)));
        idx_file_ >> *idx_buf;
        ulong data_start = idx_buf->get_ulong();
        data_file_.seekg(data_start);
        while (idx_buf->size() > idx_buf->pos()) {
            ulong data_end = idx_buf->get_ulong();
            bufptr buf = buffer::alloc(static_cast<size_t>(data_end - data_start));
            data_file_ >> *buf;
	    ptr<log_entry> entry = log_entry::deserialize(*buf);
            buf_->append(entry);
            data_start = data_end;
        }

        bufptr last_buf = buffer::alloc(static_cast<size_t>(data_file_len - data_start));
        data_file_ >> *last_buf;
	ptr<log_entry> entry = log_entry::deserialize(*last_buf);
        buf_->append(entry);
    }
}
