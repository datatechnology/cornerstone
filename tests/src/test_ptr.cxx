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

#include <cassert>
#include "cornerstone.hxx"

using namespace cornerstone;

int __ptr_test_base_calls(0);
int __ptr_test_derived_calls(0);
int __ptr_test_base_destroyed(0);
int __ptr_test_derived_destroyed(0);
int __ptr_test_circular_destroyed(0);
int __ptr_test_safe_destroyed(0);

class Base
{
public:
    Base(int val) : value_(val)
    {
    }

    virtual ~Base()
    {
        __ptr_test_base_destroyed += 1;
    }

    virtual void func()
    {
        __ptr_test_base_calls += 1;
    }

    int get_value() const
    {
        return value_;
    }

private:
    int value_;
};

class Derived : public Base
{
public:
    Derived(int val) : Base(val + 10)
    {
    }

    virtual ~Derived()
    {
        __ptr_test_derived_destroyed += 1;
    }

    virtual void func()
    {
        __ptr_test_derived_calls += 1;
    }
};

class PtrSafe : public std::enable_shared_from_this<PtrSafe>
{
public:
    PtrSafe()
    {
    }

public:
    ~PtrSafe()
    {
        __ptr_test_safe_destroyed += 1;
    }

    ptr<PtrSafe> get_this()
    {
        return shared_from_this();
    }
};

class Circular2;

class Circular1
{
public:
    ~Circular1()
    {
        __ptr_test_circular_destroyed += 1;
    }

    void set_c2(ptr<Circular2>& p)
    {
        c2_ = p;
    }

private:
    ptr<Circular2> c2_;
};

class Circular2
{
public:
    Circular2(ptr<Circular1>& c1) : c1_(c1)
    {
    }

    ~Circular2()
    {
        __ptr_test_circular_destroyed += 1;
    }

private:
    wptr<Circular1> c1_;
};

class Base1
{
public:
    virtual int func1() = 0;
};

class Base2
{
public:
    virtual int func2() = 0;
    virtual int func3() = 0;
};

class Impl : public Base1, public Base2
{
public:
    virtual ~Impl()
    {
    }

    virtual int func1()
    {
        return 1;
    }

    virtual int func2()
    {
        return 2;
    }

    virtual int func3()
    {
        return 3;
    }
};

void test_ptr()
{
    {
        wptr<Circular1> c1ref;
        {
            ptr<Base> b(cs_new<Base>(1));
            ptr<Base> b1(cs_new<Derived>(1));
            assert(b->get_value() == 1);
            assert(b1->get_value() == 11);
            b->func();
            b1->func();
            b = b1;
            b->func();

            ptr<Circular1> c1(cs_new<Circular1>());
            ptr<Circular2> c2(cs_new<Circular2>(c1));
            c1->set_c2(c2);
            c1ref = c1;
            ptr<Circular1> pc1 = c1ref.lock();
            assert(pc1);

            ptr<PtrSafe> ps(cs_new<PtrSafe>());
            ps = ps.get()->get_this();
            ps = ps.get()->get_this();
        }

        assert(c1ref.expired());
    }

    assert(__ptr_test_base_calls == 1);
    assert(__ptr_test_derived_calls == 2);
    assert(__ptr_test_base_destroyed == 2);
    assert(__ptr_test_derived_destroyed == 1);
    assert(__ptr_test_circular_destroyed == 2);
    assert(__ptr_test_safe_destroyed == 1);

    // test multiple inheritance
    cornerstone::ptr<Impl> impl(cornerstone::cs_new<Impl>());
    assert(1 == impl->func1());
    cornerstone::ptr<Base2> b2(impl);
    assert(3 == b2->func3());
    wptr<Base2> b2ref = impl;
    assert(!b2ref.expired());
    b2 = b2ref.lock();
    assert(3 == b2->func3());
}