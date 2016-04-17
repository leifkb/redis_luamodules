***********************************************
redis_scripts: Higher-level Redis Lua scripting
***********************************************

This is a Python module for higher-level Redis Lua scripting. Its key feature
is to automatically combine multiple Lua functions into a single script, so
that all of the Lua functions can call each other directly inside the Lua
interpreter.

Values to/from Lua are automatically JSON-en/decoded to allow for a wider
range of datatypes.

Note that this module does not use the KEYS array. As a result, it is
**incompatible with Redis Cluster**.

==============
Usage example:
==============

::

    from redis import Redis
    from redis_scripts import Scripts
    
    redis = Redis(...)
    
    @Scripts(redis)
    class S:
        def get_user_count(user_id):
            '''
            return tonumber(redis.call('hget', 'user_counts', user_id)) or 0
            '''
        
        def incr_user_count(user_id):
            '''
            redis.call('hincrby', 'user_counts', user_id)
            '''
        
        def incr_user_count_twice(user_id):
            '''
            S.incr_user_count()
            S.incr_user_count()
            '''
    
    print(S.get_user_count(123))
    S.incr_user_count_twice(123)
    print(S.get_user_count(123))

As you can see, Lua functions can call each other, and the syntax for calling
a Lua function is the same regardless of whether you call it from Python or
Lua. This is intended to simplify the creation of complex Lua scripts,
including use cases where much of an application's logic happens inside of Lua.

This module automatically creates Lua variables for the arguments defined on
your Python functions. Note that you must not include a ``self`` argument;
these aren't really Python methods.
