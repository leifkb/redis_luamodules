import pytest
import redis_luamodules
from redis_luamodules import LuaModule, LuaFunction
from tempfile import mkstemp
import os
from redis import Redis, ConnectionError
from subprocess import Popen
from time import sleep, time

@pytest.fixture(scope='module')
def redis(request):
    handle, path = mkstemp()
    os.close(handle)
    proc = Popen(['redis-server', '--port', '0', '--unixsocket', path])
    request.addfinalizer(proc.kill)
    redis = Redis(unix_socket_path=path)
    for i in xrange(30):
        try:
            redis.ping()
        except ConnectionError:
            sleep(0.1)
        else:
            break
    else:
        raise
    return redis

@pytest.fixture(autouse=True)
def flush_redis(request, redis):
    request.addfinalizer(redis.flushall)

@pytest.fixture
def BasicModule(redis):
    @LuaModule(redis)
    class BasicModule:
        def foo(a, b):
            '''
            return {a, b, a+b}
            '''
            
        def bar(a, b, c):
            '''
            return {a, BasicModule.foo(b, c)}
            '''

        def optarg(a, b, c=None):
            '''
            return c
            '''
        
        def vararg(a, b, *arg):
            '''
            return {b, arg[1]}
            '''
    return BasicModule

def test_basic(BasicModule):  
    assert BasicModule.bar(1, 2, 3) == [1, [2, 3, 5]]

def test_argcount_validation(BasicModule):
    with pytest.raises(TypeError):
        BasicModule.foo()
    with pytest.raises(TypeError):
        BasicModule.foo(1)
    assert BasicModule.foo(1, 2) == [1, 2, 3]
    with pytest.raises(TypeError):
        BasicModule.foo(1, 2, 3)

def test_optional_args(BasicModule):
    assert BasicModule.optarg(1, 2, 3) == 3
    assert BasicModule.optarg(1, 2) is None
    with pytest.raises(TypeError):
        BasicModule.optarg(1)
    with pytest.raises(TypeError):
        BasicModule.optarg(1, 2, 3, 4)

def test_vararg(BasicModule):
    with pytest.raises(TypeError):
        BasicModule.vararg(1)
    assert BasicModule.vararg(1, 2, 3) == [2, 3]
    assert BasicModule.vararg(1, 2, 3, 4) == [2, 3]

def test_pipeline(redis, BasicModule):
    with redis.pipeline() as pipe:
        s = pipe.register_script('return 123')
        s()
        assert BasicModule.foo(1, 2, redis=pipe) is pipe
        a, b = pipe.execute()
    assert a == 123
    assert b == [1, 2, 3]

def test_redis_setting(redis):
    @LuaModule
    class MyModule:
        def foo():
            '''
            return 123123
            '''
    
    with pytest.raises(TypeError):
        MyModule.foo()
    
    assert MyModule.foo(redis=redis) == 123123

def test_imports(redis):
    @LuaModule
    class Library:
        def foo():
            '''
            return 456456
            '''
    
    @LuaModule(redis, imports=Library)
    class LibraryConsumer:
        def foo():
            '''
            return Library.foo() + 1
            '''
    
    assert LibraryConsumer.foo() == 456457

def test_aliased_imports(redis):
    @LuaModule
    class Library:
        def foo():
            '''
            return 456456
            '''
    
    Library1 = Library
    
    @LuaModule
    class Library:
        def foo():
            '''
            return 123123
            '''
    
    Library2 = Library
    
    with pytest.raises(ValueError):
        @LuaModule(redis, imports=[Library1, Library2])
        class LibraryConsumer:
            pass
    
    @LuaModule(redis, imports=[Library1, (Library2, 'Library2')])
    class LibraryConsumer:
        def foo():
            '''
            return Library.foo() + 1
            '''
        
        def bar():
            '''
            return Library2.foo() + 1
            '''
    
    assert LibraryConsumer.foo() == 456457
    assert LibraryConsumer.bar() == 123124

def test_crazy_self_aliases(redis):
    @LuaModule(redis)
    class CrazyModule:
        def foo(n):
            '''
            if n == 3 then
                return CrazyModuleAlias0.foo(2)
            elseif n == 2 then
                return CrazyModuleAlias1.foo(1)
            elseif n == 1 then
                return CrazyModuleAlias2.foo(0)
            elseif n == 0 then
                return "hi!"
            else
                return nil
            end
            '''
    
    with pytest.raises(ValueError):
        CrazyModule._import_(CrazyModule)
    
    CrazyModule._import_([
        (CrazyModule, 'CrazyModuleAlias0'),
        (CrazyModule, 'CrazyModuleAlias1'),
        (CrazyModule, 'CrazyModuleAlias2')
    ])
    
    assert CrazyModule.foo(4) is None
    assert CrazyModule.foo(3) == "hi!"
    assert CrazyModule.foo(2) == "hi!"
    assert CrazyModule.foo(1) == "hi!"
    assert CrazyModule.foo(0) == "hi!"

def test_compile(BasicModule):
    assert BasicModule._compile_()

def test_sugarless_functions(redis):
    @LuaModule(redis)
    class MyModule:
        def sugar(n):
            '''
            return {"sugar", n+1}
            '''
        
        sugarless1 = (['n'], 'return {"sugarless1", n+2}')
        
        sugarless2 = LuaFunction(['n'], 'return {"sugarless2", (n or 100)+3}', first_optional_index=0)
    
    assert MyModule.sugar(200) == ['sugar', 201]
    assert MyModule.sugarless1(200) == ['sugarless1', 202]
    assert MyModule.sugarless2(200) == ['sugarless2', 203]
    assert MyModule.sugarless2() == ['sugarless2', 103]
    with pytest.raises(TypeError):
        MyModule.sugarless1()
    with pytest.raises(TypeError):
        MyModule.sugarless2(1, 2)

def test_sugarless_module(redis):
    f_dict = {
        'foo': ([], 'return AModule.bar()'),
        'bar': ([], 'return "bar"')
    }
    MyModule = LuaModule('AModule', f_dict)
    with pytest.raises(TypeError):
        MyModule.foo()
    assert MyModule.foo(redis=redis) == 'bar'
    
    MyModule = LuaModule('AModule', redis, f_dict)
    assert MyModule.foo() == 'bar'

def test_now(redis):
    @LuaModule(redis)
    class MyModule:
        def now():
            '''
            return NOW
            '''
    
    assert abs(MyModule.now() - time()) < 0.1
    sleep(1)
    assert abs(MyModule.now() - time()) < 0.1
