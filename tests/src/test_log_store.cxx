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
#include <cassert>

using namespace cornerstone;

#ifdef _WIN32
#define LOG_INDEX_FILE "\\store.idx"
#define LOG_DATA_FILE "\\store.dat"
#define LOG_START_INDEX_FILE "\\store.sti"
#define LOG_INDEX_FILE_BAK "\\store.idx.bak"
#define LOG_DATA_FILE_BAK "\\store.dat.bak"
#define LOG_START_INDEX_FILE_BAK "\\store.sti.bak"

#include <Windows.h>

int mkdir(const char* path, int mode) {
    (void)mode;
    return 1 == ::CreateDirectoryA(path, NULL) ? 0 : -1;
}

int rmdir(const char* path) {
    return 1 == ::RemoveDirectoryA(path) ? 0 : -1;
}
#undef min
#undef max
#else
#define LOG_INDEX_FILE "/store.idx"
#define LOG_DATA_FILE "/store.dat"
#define LOG_START_INDEX_FILE "/store.sti"
#define LOG_INDEX_FILE_BAK "/store.idx.bak"
#define LOG_DATA_FILE_BAK "/store.dat.bak"
#define LOG_START_INDEX_FILE_BAK "/store.sti.bak"

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

static void cleanup(const std::string& folder) {
    std::remove((folder + LOG_INDEX_FILE).c_str());
    std::remove((folder + LOG_DATA_FILE).c_str());
    std::remove((folder + LOG_START_INDEX_FILE).c_str());
    std::remove((folder + LOG_INDEX_FILE_BAK).c_str());
    std::remove((folder + LOG_DATA_FILE_BAK).c_str());
    std::remove((folder + LOG_START_INDEX_FILE_BAK).c_str());
}

static void cleanup() {
    cleanup(".");
}

static ptr<log_entry> rnd_entry(std::function<int32()>& rnd) {
    ptr<buffer> buf = buffer::alloc(rnd() % 100 + 8);
    for (size_t i = 0; i < buf->size(); ++i) {
        buf->put(static_cast<byte>(rnd() % 256));
    }

    buf->pos(0);
    log_val_type t = (log_val_type)(rnd() % 5 + 1);
    return cs_new<log_entry>(rnd(), buf, t);
}

static bool entry_equals(log_entry& entry1, log_entry& entry2) {
    bool result = entry1.get_term() == entry2.get_term() 
                    && entry1.get_val_type() == entry2.get_val_type()
                    && entry1.get_buf().size() == entry2.get_buf().size();
    if (result) {
        for (size_t i = 0; i < entry1.get_buf().size(); ++i) {
            byte b1 = entry1.get_buf().data()[i];
            byte b2 = entry2.get_buf().data()[i];
            result = b1 == b2;
        }
    }

    return result;
}

void test_log_store() {
    uint seed = (uint)std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<int32> distribution(1, 10000);
    std::function<int32()> rnd = std::bind(distribution, engine);

    cleanup();
    fs_log_store store(".", 100);

    // initialization test
    ptr<log_entry> entry(store.last_entry());
    assert(entry->get_term() == 0);
    assert(store.next_slot() == 1);
    assert(store.entry_at(1) == nilptr);

    std::vector<ptr<log_entry>> logs;
    for (int i = 0; i < 100 + rnd() % 100; ++i) {
        ptr<log_entry> item(rnd_entry(rnd));
        store.append(item);
        logs.push_back(item);
    }

    // overall test
    assert(logs.size() == static_cast<size_t>(store.next_slot() - 1));
    ptr<log_entry> last(store.last_entry());
    assert(entry_equals(*last, *(logs[logs.size() - 1])));

    // random item test
    for (int i = 0; i < 20; ++i) {
        size_t idx = (size_t)(rnd() % logs.size());
        ptr<log_entry> item(store.entry_at(idx + 1));
        assert(entry_equals(*item, *(logs[idx])));
    }

    // random range test
    size_t rnd_idx = (size_t)rnd() % logs.size();
    size_t rnd_sz = (size_t)rnd() % (logs.size() - rnd_idx);
    ptr<std::vector<ptr<log_entry>>> entries = store.log_entries(rnd_idx + 1, rnd_idx + rnd_sz + 1);
    for (size_t i = rnd_idx; i < rnd_idx + rnd_sz; ++i) {
        assert(entry_equals(*(logs[i]), *(*entries)[i - rnd_idx]));
    }
    
    store.close();
    fs_log_store store1(".", 100);
    // overall test
    assert(logs.size() == static_cast<size_t>(store1.next_slot() - 1));
    ptr<log_entry> last1(store1.last_entry());
    assert(entry_equals(*last1, *(logs[logs.size() - 1])));

    // random item test
    for (int i = 0; i < 20; ++i) {
        size_t idx = (size_t)rnd() % logs.size();
        ptr<log_entry> item(store1.entry_at(idx + 1));
        assert(entry_equals(*item, *(logs[idx])));
    }

    // random range test
    rnd_idx = (size_t)rnd() % logs.size();
    rnd_sz = (size_t)rnd() % (logs.size() - rnd_idx);
    ptr<std::vector<ptr<log_entry>>> entries1(store1.log_entries(rnd_idx + 1, rnd_idx + rnd_sz + 1));
    for (size_t i = rnd_idx; i < rnd_idx + rnd_sz; ++i) {
        assert(entry_equals(*(logs[i]), *(*entries1)[i - rnd_idx]));
    }

    // test write at
    ptr<log_entry> item(rnd_entry(rnd));
    rnd_idx = rnd() % store1.next_slot() + 1;
    store1.write_at(rnd_idx, item);
    assert(rnd_idx + 1 == store1.next_slot());
    ptr<log_entry> last2(store1.last_entry());
    assert(entry_equals(*item, *last2));
    store1.close();
    fs_log_store store2(".", 100);
    ptr<log_entry> last3(store2.last_entry());
    assert(entry_equals(*item, *last3));
    store2.close();
    cleanup();
}

void test_log_store_buffer() {
    uint seed = (uint)std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<int32> distribution(1, 10000);
    std::function<int32()> rnd = std::bind(distribution, engine);

    cleanup();
    fs_log_store store(".", 1000);
    int logs_count = rnd() % 1000 + 1500;
    std::vector<ptr<log_entry>> entries;
    for (int i = 0; i < logs_count; ++i) {
        ptr<log_entry> entry(rnd_entry(rnd));
        store.append(entry);
        entries.push_back(entry);
    }

    int start = rnd() % (logs_count - 1000);
    int end = logs_count - 500;
    ptr<std::vector<ptr<log_entry>>> results = store.log_entries((ulong)start + 1, (ulong)end + 1);
    for (int i = start; i < end; ++i) {
        entry_equals(*entries[i], *(*results)[i - start]);
    }

    store.close();
    cleanup();
}

void test_log_store_pack() {
    uint seed = (uint)std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<int32> distribution(1, 10000);
    std::function<int32()> rnd = std::bind(distribution, engine);

    cleanup();
    cleanup("tmp");
    mkdir("tmp", 0766);
    fs_log_store store(".", 1000);
    fs_log_store store1("tmp", 1000);
    int logs_cnt = rnd() % 1000 + 1000;
    for (int i = 0; i < logs_cnt; ++i) {
        ptr<log_entry> entry = rnd_entry(rnd);
        store.append(entry);
        entry = rnd_entry(rnd);
        store1.append(entry);
    }

    int logs_copied = 0;
    while (logs_copied < logs_cnt) {
        ptr<buffer> pack = store.pack(logs_copied + 1, 100);
        store1.apply_pack(logs_copied + 1, *pack);
        logs_copied = std::min(logs_copied + 100, logs_cnt);
    }

    assert(store1.next_slot() == store.next_slot());
    for (int i = 1; i <= logs_cnt; ++i) {
        ptr<log_entry> entry1 = store.entry_at((ulong)i);
        ptr<log_entry> entry2 = store1.entry_at((ulong)i);
        assert(entry_equals(*entry1, *entry2));
    }

    store.close();
    store1.close();
    cleanup();
    cleanup("tmp");
    rmdir("tmp");
}

void test_log_store_compact_all() {
    uint seed = (uint)std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<int32> distribution(1, 10000);
    std::function<int32()> rnd = std::bind(distribution, engine);
    
    cleanup();
    fs_log_store store(".", 1000);
    int cnt = rnd() % 1000 + 100;
    std::vector<ptr<log_entry>> entries;
    for (int i = 0; i < cnt; ++i) {
        ptr<log_entry> entry(rnd_entry(rnd));
        store.append(entry);
        entries.push_back(entry);
    }

    assert(1 == store.start_index());
    assert(entries.size() == (size_t)store.next_slot() - 1);
    assert(entry_equals(*entries[entries.size() - 1], *store.last_entry()));
    ulong last_idx = entries.size();
    assert(store.compact(last_idx));
    assert(entries.size() + 1 == (size_t)store.start_index());
    assert(entries.size() == (size_t)store.next_slot() - 1);

    cnt = rnd() % 100 + 10;
    for (int i = 0; i < cnt; ++i) {
        ptr<log_entry> entry(rnd_entry(rnd));
        entries.push_back(entry);
        store.append(entry);
    }

    assert(last_idx + 1 == (size_t)store.start_index());
    assert(entries.size() == (size_t)store.next_slot() - 1);
    assert(entry_equals(*entries[entries.size() - 1], *store.last_entry()));

    ulong index = store.start_index() + rnd() % (store.next_slot() - store.start_index());
    assert(entry_equals(*entries[(size_t)index - 1], *store.entry_at(index)));
    store.close();
    cleanup();
}

void test_log_store_compact_random() {
    uint seed = (uint)std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<int32> distribution(1, 10000);
    std::function<int32()> rnd = std::bind(distribution, engine);

    cleanup();
    fs_log_store store(".", 1000);
    int cnt = rnd() % 1000 + 100;
    std::vector<ptr<log_entry>> entries;
    for (int i = 0; i < cnt; ++i) {
        ptr<log_entry> entry(rnd_entry(rnd));
        store.append(entry);
        entries.push_back(entry);
    }

    ulong last_log_idx = entries.size();
    ulong idx_to_compact = rnd() % (last_log_idx - 10) + 1;
    assert(store.compact(idx_to_compact));

    assert(store.start_index() == (idx_to_compact + 1));
    assert(store.next_slot() == (entries.size() + 1));

    for (size_t i = 0; i < store.next_slot() - idx_to_compact - 1; ++i) {
        ptr<log_entry> entry = store.entry_at(store.start_index() + i);
        assert(entry_equals(*entry, *entries[i + (size_t)idx_to_compact]));
    }

    ulong rnd_idx = (ulong)rnd() % (store.next_slot() - store.start_index()) + store.start_index();
    ptr<log_entry> entry = rnd_entry(rnd);
    store.write_at(rnd_idx, entry);
    entries[(size_t)rnd_idx - 1] = entry;
    entries.erase(entries.begin() + (size_t)rnd_idx, entries.end());

    for (size_t i = 0; i < store.next_slot() - idx_to_compact - 1; ++i) {
        ptr<log_entry> entry = store.entry_at(store.start_index() + i);
        assert(entry_equals(*entry, *entries[i + (size_t)idx_to_compact]));
    }

    cnt = rnd() % 100 + 10;
    for (int i = 0; i < cnt; ++i) {
        ptr<log_entry> entry = rnd_entry(rnd);
        entries.push_back(entry);
        store.append(entry);
    }

    for (size_t i = 0; i < store.next_slot() - idx_to_compact - 1; ++i) {
        ptr<log_entry> entry = store.entry_at(store.start_index() + i);
        assert(entry_equals(*entry, *entries[i + (size_t)idx_to_compact]));
    }
    store.close();
    cleanup();
}
