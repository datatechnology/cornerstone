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
#include <cstdlib>
#include <cassert>
#include <cstring>

using namespace cornerstone;

void test_strfmt() {
    assert(0 == strcmp("number 10", strfmt<20>("number %d").fmt(10)));
    assert(0 == strcmp("another string", strfmt<30>("another %s").fmt("string")));
    assert(0 == strcmp("100-20=80", strfmt<30>("%d-%d=%d").fmt(100, 20, 80)));
}
