import pytest
import redis_luamodules
from redis_luamodules import LuaModule, LuaFunction
from tempfile import mkstemp
import os
from redis import Redis, ConnectionError
from subprocess import Popen
from time import sleep

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
    
    @LuaModule(redis, imports=(Library, 'AliasOfLibrary'))
    class LibraryConsumer:
        def foo():
            '''
            return AliasOfLibrary.foo() + 1
            '''
    
    assert LibraryConsumer.foo() == 456457
