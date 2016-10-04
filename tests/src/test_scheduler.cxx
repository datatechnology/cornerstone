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
#include <cassert>

using namespace cornerstone;
int _timer_fired_counter = 0;
void test_scheduler() {
    asio_service svc;
    timer_task<void>::executor handler = (std::function<void()>)([]() -> void {
        _timer_fired_counter++;
    });
    ptr<delayed_task> task(cs_new<timer_task<void>>(handler));
    std::cout << "scheduled to wait for 200ms" << std::endl;
    svc.schedule(task, 200);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    assert(_timer_fired_counter == 1);

    std::cout << "scheduled to wait for 300ms" << std::endl;
    svc.schedule(task, 300);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "cancel the timer task" << std::endl;
    svc.cancel(task);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    assert(_timer_fired_counter == 1);

    std::cout << "scheduled to wait for 300ms" << std::endl;
    svc.schedule(task, 300);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    assert(_timer_fired_counter == 2);
    svc.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}