from redis.client import BasePipeline
from functools import partial
from inspect import isclass, getargspec
try:
    import ujson as json
except:
    import json
from time import time

def _pipeline_response_callback(result, **kwargs):
    if kwargs.get('luamodule'):
        return json.loads(result)
    else:
        return result

class LuaFunction(object):
    '''Create a LuaFunction like:
    LuaFunction(['arg1', 'arg2'], 'return {"hello from lua", arg1+arg2}')
    
    Constructor supports two optional arguments:
    first_optional_index (all optional arguments have a nil default value)
    varargs (boolean, will use standard Lua varargs scheme)
    '''
    
    def __init__(self, arg_names, code, first_optional_index=None, varargs=False):
        self.code = code
        self.arg_names = arg_names
        self.first_optional_index = first_optional_index
        self.varargs = varargs
    
    @classmethod
    def from_obj(cls, obj):
        '''Converts an object to a LuaFunction, if possible. Supported objects:
        
        A 2-tuple: (arg_names, lua_code)
        A LuaFunction (which will be returned unmodified)
        A Python function (its argspec will be appropriated, and its docstring
        should contain Lua code)
        
        Raises TypeError if this is not a supported type of object.
        '''
        
        if isinstance(obj, LuaFunction):
            return obj
        
        try:
            arg_names, lua_code = obj
        except (TypeError, ValueError):
            pass
        else:
            return cls(arg_names, lua_code)
        
        # This will raise TypeError if it's not a function/method.
        argspec = getargspec(obj)
        
        return cls.from_python_argspec(obj.__doc__, argspec)
    
    @classmethod
    def from_python_argspec(cls, code, argspec):
        if not all(isinstance(arg, basestring) for arg in argspec.args):
            raise TypeError('Lua function do not support unpacking.')
        arg_names = argspec.args
        if argspec.varargs:
            if argspec.varargs != 'arg':
                raise TypeError('Lua vararg variable must be named "arg".')
            varargs = True
        else:
            varargs = False
        if argspec.keywords:
            raise TypeError('LuaModule does not support kwargs on Lua functions.')
        if argspec.defaults is None:
            first_optional_index = None
        else:
            if not all(default is None for default in argspec.defaults):
                raise TypeError('Any default argument values for Lua functions must be None.')
            first_optional_index = len(arg_names) - len(argspec.defaults)
        return cls(arg_names, code, first_optional_index, varargs)
    
    @property
    def arg_count_range(self):
        """Returns (min_args, max_args) tuple, where max_args can be None."""
        
        if self.first_optional_index is None:
            min_args = len(self.arg_names)
        else:
            min_args = min(self.first_optional_index, len(self.arg_names))
        if self.varargs:
            max_args = None
        else:
            max_args = len(self.arg_names)
        return min_args, max_args
    
    @property
    def arg_count_range_text(self):
        """Text describing the arg count range."""
        
        min_args, max_args = self.arg_count_range
        if min_args == max_args:
            return 'exactly %s' % min_args
        elif max_args is None:
            return 'at least %s' % min_args
        else:
            return 'between %s and %s' % (min_args, max_args)
    
    def arg_count_valid(self, n):
        min_args, max_args = self.arg_count_range
        if n < min_args:
            return False
        if max_args is not None and n > max_args:
            return False
        return True
    
    @property
    def lua_argdef(self):
        arg_names = self.arg_names
        if self.varargs:
            arg_names = list(arg_names)
            arg_names.append('...')
        return ', '.join(arg_names)
    
    @property
    def lua_funcdef(self):
        return 'function(%s) %s end' % (self.lua_argdef, self.code)

class LuaModule(object):
    '''A LuaModule can be created using a decorator as syntactic sugar like:
    
    @LuaModule
    class MyModule:
        def my_function(x, y, z):
            """
            return x+y*z
            """
    
    (Note that there should not be a "self" argument.)
    
    Individual Lua functions can also be defined as (arg_names, code) tuples,
    or as LuaFunction objects. This allows code to be dynamically generated, or
    loaded from a file:
    
    @LuaModule:
    class MyModule:
        my_function = (['x', 'y', 'z'], 'return x+y*z')
    
    Alternatively, the whole LuaModule can be defined without syntactic sugar:
    
    MyModule = LuaModule('MyModule', {
        'my_function': (['x', 'y', 'z'], 'return x+y*z')
    })
    
    Because LuaModule functions are called like methods (MyModule.my_function()),
    all Python methods are prefixed with an underscore to avoid conflict.
    Methods with underscores on _both_sides_ are intended as external API; e.g.,
    you can see the compiled Lua code of a LuaModule like:
    
    print(MyModule._compile_())
    
    And you can add imports to a module like:
    
    MyModule._import_(ModuleA)
    MyModule._import_(('ModuleA', 'AliasA'))
    MyModule._import_([ModuleA, (ModuleB, 'AliasB')])
    '''
    
    def __new__(cls, *args, **kwargs):
        if len(args) == 0:
            return partial(LuaModule, **kwargs)
        elif len(args) == 1 and isclass(args[0]):
            return LuaModule(None, args[0], **kwargs)
        elif len(args) == 1: # redis
            return partial(LuaModule, args[0], **kwargs)

        if len(args) == 2:
            redis_or_name, functions = args
            if isinstance(redis_or_name, basestring):
                redis = None
                name = redis_or_name
            else:
                redis = redis_or_name
                name = None
        elif len(args) == 3:
            name, redis, functions = args
        else:
            raise TypeError('Wrong number of args')
        
        if name is None:
            name = kwargs.pop('name', None)
        if name is None:
            name = getattr(functions, '__name__', None)
        if name is None:
            raise TypeError('Name not specified.')
        
        if redis is None:
            redis = kwargs.pop('redis', None)
        
        imports = kwargs.pop('imports', None)
        if kwargs:
            raise TypeError('Excess kwargs: %s' % kwargs)
        
        self = object.__new__(cls)
        self._name_ = name
        self._compile_called = False
        self._imports_ = [(self, self._name_)]
        if imports:
            self._import_(imports)
        if isclass(functions):
            functions = self._extract_lua_from_class(functions)
        else:
            functions = dict(functions)
            for f_name, f in functions.iteritems():
                functions[f_name] = LuaFunction.from_obj(f)
        self._functions_ = dict(functions)
        self._registered_script = None
        self._redis_ = redis
        return self

    @classmethod
    def _extract_lua_from_class(cls, other_cls):
        for method_name in dir(other_cls):
            if method_name.endswith('_'):
                continue
            method = getattr(other_cls, method_name)
            try:
                f = LuaFunction.from_obj(method)
            except TypeError:
                continue
            yield method_name, f
    
    def _compile_module(self, module_map):
        self._compile_called = True
        import_names = []
        global_names = []
        for module, import_as in self._imports_:
            import_names.append(import_as)
            global_names.append(module_map[module])
        set_functions = '\n'.join(
            '%s[%r] = %s' % (self._name_, func_name, func.lua_funcdef)
            for func_name, func in self._functions_.iteritems()
        )
        return '''
        do
            local {import_names} = {global_names}
            {set_functions}
        end
        '''.format(
            import_names=', '.join(import_names),
            global_names=', '.join(global_names),
            set_functions=set_functions
        )
    
    def _all_imports_recurse(self, s):
        s.add(self)
        for module, import_as in self._imports_:
            if module not in s:
                module._all_imports_recurse(s)
    
    def _all_imports(self):
        s = set()
        self._all_imports_recurse(s)
        return s
    
    def _compile_(self):
        all_imports = self._all_imports()
        n = 0
        module_map = {}
        for module in all_imports:
            module_map[module] = '_LuaModule%s__%s' % (n, module._name_)
            n += 1
        set_modules = '\n'.join(module._compile_module(module_map) for module in module_map)
        return '''
        local NOW = tonumber(ARGV[3])
        local {module_names} = {blank_tables}
        {set_modules}
        return cjson.encode({own_name}[ARGV[1]](unpack(cjson.decode(ARGV[2]))) or nil)
        '''.format(
            module_names=', '.join(module_map.itervalues()),
            blank_tables=', '.join(['{}'] * len(module_map)),
            set_modules=set_modules,
            own_name=module_map[self]
        )
    
    def _import_name_used(self, name):
        return any(import_as == name for module, import_as in self._imports_)
    
    def _import_(self, imports):
        if self._compile_called:
            raise TypeError('Attempted to add imports to LuaModule after it has already been used')
        if isinstance(imports, LuaModule) or (isinstance(imports, tuple) and len(imports) == 2 and isinstance(imports[1], basestring)):
            imports = [imports]
        for an_import in imports:
            if isinstance(an_import, LuaModule):
                module = an_import
                import_as = module._name_
            else:
                module, import_as = an_import
            if self._import_name_used(import_as):
                raise ValueError("LuaModule %s tried to re-use import name %s" % (self._name_, import_as))
            self._imports_.append((module, import_as))
    
    def _call_(self, name, *args, **kwargs):
        redis = kwargs.pop('redis', None)
        if kwargs:
            raise TypeError('Excess kwargs: %s' % kwargs.keys())
        func = self._functions_[name]
        if not func.arg_count_valid(len(args)):
            raise TypeError('Wrong number of args to LuaModule %s.%s; expected %s, got %s' % (self._name_, name, func.arg_count_range_text, len(args)))
        redis = redis or self._redis_
        if redis is None:
            raise TypeError('This LuaModule was created without a default Redis client, so you need to specify one in a kwarg: %s.%s(..., redis=redis)' % (self._name_, name))
        if self._registered_script is None:
            self._registered_script = redis.register_script(self._compile_())
        args = json.dumps(args)
        if isinstance(redis, BasePipeline):
            # Messing with redis-py's internals a bit to support pipelines,
            # but I think in practice, this is the best way to do it.
            redis.set_response_callback('EVALSHA', _pipeline_response_callback)
            redis.evalsha = partial(redis.execute_command, 'EVALSHA', luamodule=True)
            self._registered_script(args=[name, args, time()], client=redis)
            del redis.evalsha
            return redis
        else:
            return json.loads(self._registered_script(args=[name, args, time()], client=redis))

    def __getattr__(self, name):
        if name.startswith('_') or name.endswith('_') or name not in self._functions_:
            raise AttributeError(name)
        return partial(self._call_, name)
