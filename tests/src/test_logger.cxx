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
#include <fstream>
#include <iostream>

using namespace cornerstone;

void test_logger() {
    std::string text[4];
    text[0] = "line1";
    text[1] = "line2";
    text[2] = "line3";
    text[3] = "line4";
    {
        asio_service svc;
        ptr<logger> l(svc.create_logger(asio_service::log_level::debug, "log1.log"));
        l->debug(text[0]);
        l->info(text[1]);
        l->warn(text[2]);
        l->err(text[3]);
        svc.stop();
    }

    std::ifstream log1("log1.log");
    for (int i = 0; i < 4; ++i) {
        std::string line;
        std::getline(log1, line);
        std::string log_text = line.substr(line.length() - 5);
        assert(log_text == text[i]);
    }

    log1.close();
    {
        asio_service svc;
        ptr<logger> l(svc.create_logger(asio_service::log_level::warnning, "log2.log"));
        l->debug(text[0]);
        l->info(text[1]);
        l->warn(text[2]);
        l->err(text[3]);
        svc.stop();
    }

    std::ifstream log2("log2.log");
    for (int i = 2; i < 4; ++i) {
        std::string line;
        std::getline(log2, line);
        std::string log_text = line.substr(line.length() - 5);
        assert(log_text == text[i]);
    }

    log2.close();

    std::remove("log1.log");
    std::remove("log2.log");
}