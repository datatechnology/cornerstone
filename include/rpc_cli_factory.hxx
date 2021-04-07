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

#ifndef _RPC_CLIENT_FACTORY_HXX_
#define _RPC_CLIENT_FACTORY_HXX_

namespace cornerstone {
    class rpc_client_factory {
    __interface_body__(rpc_client_factory)
    public:
        virtual ptr<rpc_client> create_client(const std::string& endpoint) = 0;
    };
}

#endif
