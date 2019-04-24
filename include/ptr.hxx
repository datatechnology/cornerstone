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

#ifndef _CS_PTR_HXX_
#define _CS_PTR_HXX_
namespace cornerstone {
    template<typename T>
    using ptr = std::shared_ptr<T>;

    template<typename T>
    using wptr = std::weak_ptr<T>;

    template<typename T, typename Deleter = std::default_delete<T>>
    using uptr = std::unique_ptr<T, Deleter>;

    template<typename T, typename ... TArgs>
    inline ptr<T> cs_new(TArgs&&... args) {
        return std::make_shared<T>(std::forward<TArgs>(args)...);
    }
}
#endif //_CS_PTR_HXX_
