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
#include <iostream>

#define __decl_test__(name) void test_##name()
#define __run_test__(name)  \
    std::cout << "run test: " << #name << "..." << std::endl;\
    test_##name();\
    std::cout << "test " << #name << " passed." << std::endl

__decl_test__(async_result);
__decl_test__(strfmt);
__decl_test__(buffer);
__decl_test__(serialization);
__decl_test__(scheduler);
__decl_test__(logger);
__decl_test__(log_store);
__decl_test__(raft_server);
__decl_test__(log_store);
__decl_test__(ptr);
__decl_test__(log_store_buffer);
__decl_test__(log_store_pack);
__decl_test__(log_store_compact_all);
__decl_test__(log_store_compact_random);

int main() {
    __run_test__(async_result);
    __run_test__(strfmt);
    __run_test__(buffer);
    __run_test__(serialization);
    __run_test__(scheduler);
    __run_test__(logger);
    __run_test__(log_store);
    __run_test__(log_store_buffer);
    __run_test__(log_store_pack);
    __run_test__(log_store_compact_all);
    __run_test__(log_store_compact_random);
    __run_test__(ptr);
    __run_test__(raft_server);
    return 0;
}