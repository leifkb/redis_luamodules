***********************************************
redis_luamodules: Higher-level Redis Lua scripting
***********************************************

This is a Python module for higher-level Redis Lua scripting. Its key feature
is to automatically combine multiple Lua functions into a single script, so
that your Lua functions can call each other directly. This is intended to
allow more code reuse, and to simplify the creation of complex Lua scripts,
including use cases where much of an application's logic happens inside of
a Redis instance's Lua interpreter.

Note that this module does not use the ``KEYS`` array. As a result, it is
**incompatible with Redis Cluster**.

===============
Usage examples:
===============

::

    from redis import Redis
    from redis_luamodules import LuaModule
    
    redis = Redis(...)
    
    @LuaModule(redis)
    class MyModule:
        def get_user_count(user_id):
            '''
            return tonumber(redis.call('hget', 'user_counts', user_id)) or 0
            '''
        
        def incr_user_count(user_id):
            '''
            redis.call('hincrby', 'user_counts', user_id, 1)
            '''
        
        def incr_user_count_twice(user_id):
            '''
            MyModule.incr_user_count(user_id)
            MyModule.incr_user_count(user_id)
            '''
    
    print(MyModule.get_user_count(123)) # prints 0
    MyModule.incr_user_count_twice(123)
    print(MyModule.get_user_count(123)) # prints 2

As you can see, the syntax for calling a Lua function is the same regardless
of whether you call it from Python or Lua. Lua-to-Lua function calls are just
that -- normal Lua function calls -- so they should be quite efficient, and you
can refactor your Lua code into smaller functions as desired.

``LuaModule`` automatically creates Lua variables for the arguments defined on
your Python functions. Note that you **must not** include a ``self`` argument;
these aren't really Python methods (and you're not really creating a class --
the decorator replaces the class with an instance of LuaModule).

Values passed between Lua and Python are automatically (and transparently)
JSON-serialized to allow for a wider range of datatypes.

You can import one ``LuaModule`` into another and call the imported module's
functions from Lua::

    @LuaModule
    class MyLibrary:
        def add_numbers(a, b):
            '''
            return a + b
            '''
    
    @LuaModule(redis, imports=MyLibrary)
    class MyModule:
        def add_and_double(a, b):
            '''
            return MyLibrary.add_numbers(a, b) * 2
            '''

Notice that  ``MyLibrary`` is defined without a default Redis client object.
If you tried to call it directly from Python like ``MyLibrary.add_numbers(2, 2)``,
you would get an error. However, you can still call it by passing a Redis
client as a keyword argument: ``MyLibrary.add_numbers(2, 2, redis=redis)``.

Imports support all the features you would expect, including multiple imports
(use a list), aliases (use a tuple, like
``imports=[(MyLibrary, 'MyLibraryAlias')]``), and even cyclical imports, which
require you to call the ``_import_`` method of a ``LuaModule`` after it has
been defined::

    @LuaModule
    class ModuleA:
        def foo():
            '''
            return "called ModuleA.foo"
            '''
        
        def bar():
            '''
            return ModuleB.bar()
            '''
    
    @LuaModule(imports=ModuleA)
    class ModuleB:
        def foo():
            '''
            return ModuleA.foo()
            '''
        
        def bar():
            '''
            return "called ModuleB.bar"
            '''
    
    ModuleA._import_(ModuleB)
            
