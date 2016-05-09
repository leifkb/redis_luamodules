***********************************************
redis_luamodules: Higher-level Redis Lua scripting
***********************************************

This is a Python library for higher-level Redis Lua scripting. Its key feature
is to automatically combine multiple Lua functions into a single script,
allowing your Lua functions to call each other directly. This allows more code
reuse, and simplifies the creation of complex Lua scripts, including use
cases where all of a program's Redis interaction is done through Lua.

Note that this library does not use the ``KEYS`` array. As a result, it is
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
that – normal Lua function calls – so they should be quite efficient, and you
can refactor your Lua code into smaller functions as desired.

``LuaModule`` automatically creates Lua variables for the arguments defined on
your Python functions. Note that you **must not** include a ``self`` argument;
these aren't really Python methods. (And you're not really creating a class: the
``@LuaModule`` decorator replaces the class with an instance of ``LuaModule``.)

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
    
    print(MyModule.add_and_double(2, 3))
    # prints 10
    
    print(MyLibrary.add_numbers(2, 2))
    # error: MyLibrary doesn't have a default redis client
    
    print(MyLibrary.add_numbers(2, 2, redis=redis))
    # prints 4

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

Individual Lua functions can be defined without syntactic sugar, as either a
tuple ``(arg_list, lua_code)`` or a ``LuaFunction`` object. This is useful if
you want to generate the code for a Lua function dynamically, or to load it
from a file::

  @LuaModule
  class MyModule:
      def foo():
          '''
          return 1
          '''
      
      bar = ([], 'return 2')
      
      baz = LuaFunction(['x'], 'return x or 3', first_optional_index=0)

It is also possible to define an entire ``LuaModule`` without syntactic sugar::

    MyModule = LuaModule('MyModule', {
        'foo': ([], 'return 1'),
        'bar': LuaFunction([], 'return 2')  
    })

To see the generated Lua code, use the ``_compile_()`` method of a ``LuaModule``::

    print(MyModule._compile_())
