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

#include <utility>
#include <iostream>
#include <thread>
#include <chrono>
#include <cassert>
#include "../../include/cornerstone.hxx"

using namespace cornerstone;

typedef async_result<int>::handler_type int_handler;

ptr<std::exception> no_except;

ptr<async_result<int>> create_and_set_async_result(int time_to_sleep, int value, ptr<std::exception>& err) {
    ptr<async_result<int>> result(cs_new<async_result<int>>());
    if (time_to_sleep <= 0) {
        result->set_result(value, err);
        return result;
    }

    std::thread th([=]() -> void {
        std::this_thread::sleep_for(std::chrono::milliseconds(time_to_sleep));
        ptr<std::exception> ex(err);
        result->set_result(value, ex);
    });

    th.detach();
    return result;
}

void test_async_result() {
    std::cout << "test with sync set" << std::endl;
    {
        ptr<async_result<int>> p(create_and_set_async_result(0, 123, no_except));
        assert(123 == p->get());
        bool handler_called = false;	
        p->when_ready([&handler_called](int val, const ptr<std::exception>& e) -> void {
            assert(123 == val);
            assert(e == nullptr);
            handler_called = true;
	    });
        assert(handler_called);
    }

    std::cout << "test with async set and wait" << std::endl;
    {
        ptr<async_result<int>> presult(create_and_set_async_result(200, 496, no_except));
        bool handler_called = false;
        presult->when_ready([&handler_called](int val, const ptr<std::exception>& e) -> void {
            assert(496 == val);
            assert(e == nullptr);
            handler_called = true;
            });
        assert(496 == presult->get());
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        assert(handler_called);
    }

    std::cout << "test with async set and wait without completion handler" << std::endl;
    {
        ptr<async_result<int>> p(create_and_set_async_result(200, 496, no_except));
        assert(496 == p->get());
    }

    std::cout << "test with exceptions" << std::endl;
    {
        ptr<std::exception> ex = cs_new<std::bad_exception>();
        ptr<async_result<int>> presult(create_and_set_async_result(200, 496, ex));
        bool handler_called = false;
        presult->when_ready([&handler_called, ex](int, const ptr<std::exception>& e) -> void {
            assert(ex == e);
            handler_called = true;
            });

        bool ex_handled = false;
        try {
            presult->get();
        }
        catch (const ptr<std::exception>& err) {
            (void)err;
            assert(ex == err);
            ex_handled = true;
        }

        assert(ex_handled);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        assert(handler_called);
    }
}
