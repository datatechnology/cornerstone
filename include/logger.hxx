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

#ifndef _LOGGER_HXX_
#define _LOGGER_HXX_

namespace cornerstone {
    class logger {
    __interface_body__(logger)

    public:
        virtual void debug(const std::string& log_line) = 0;
        virtual void info(const std::string& log_line) = 0;
        virtual void warn(const std::string& log_line) = 0;
        virtual void err(const std::string& log_line) = 0;
    };
}

#endif //_LOGGER_HXX_