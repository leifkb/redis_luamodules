from redis.client import BasePipeline
from functools import partial
from inspect import isclass, getargspec
try:
    import ujson as json
except:
    import json

def _pipeline_response_callback(result, **kwargs):
    if kwargs.get('luamodule'):
        return json.loads(result)
    else:
        return result

class LuaFunction(object):
    def __init__(self, code, arg_names, first_optional_index=None, varargs=False):
        self.code = code
        self.arg_names = arg_names
        self.first_optional_index = first_optional_index
        self.varargs = varargs
    
    @classmethod
    def from_python_argspec(cls, code, argspec):
        if not all(isinstance(arg, basestring) for arg in argspec.args):
            raise TypeError('Lua function do not support unpacking.')
        arg_names = argspec.args
        varargs = bool(argspec.varargs)
        if argspec.keywords:
            raise TypeError('LuaModule does not support kwargs on Lua functions.')
        if argspec.defaults is None:
            first_optional_index = None
        else:
            if not all(default is None for default in argspec.defaults):
                raise TypeError('Any default argument values for Lua functions must be None.')
            first_optional_index = len(arg_names) - len(argspec.defaults)
        return cls(code, arg_names, first_optional_index, varargs)
    
    @property
    def arg_count_range(self):
        """Returns (min_args, max_args) tuple, where max_args can be None."""
        
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

class LuaModule(object):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0 and kwargs:
            return partial(LuaModule, **kwargs)
        elif len(args) == 1 and isclass(args[0]):
            return LuaModule(None, args[0], **kwargs)
        elif len(args) == 1: # redis
            return partial(LuaModule, args[0], **kwargs)
        else:
            if len(args) == 2:
                redis, functions = args
            else:
                raise TypeError('Wrong number of args')
            table_name = kwargs.pop('name', None)
            if table_name is None:
                table_name = getattr(functions, '__name__', 'S')
            imports = kwargs.pop('imports', None)
            if kwargs:
                raise TypeError('Excess kwargs: %s' % kwargs)
            
            self = object.__new__(cls)
            self._name_ = table_name
            self._compile_called = False
            self._imports_ = []
            if imports:
                self._import_(imports)
            if isclass(functions):
                functions = self._extract_lua_from_class(functions)
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
                argspec = getargspec(method)
            except TypeError:
                continue
            yield method.__name__, LuaFunction.from_python_argspec(method.__doc__, argspec)
    
    def _compile_module(self, module_map):
        self._compile_called = True
        import_as_names = [import_as for module, import_as in self._imports_]
        import_as_names.append(self._name_)
        import_as_names = ', '.join(import_as_names)
        set_modules = '\n'.join('%s = %s' % (import_as, module_map[module]) for module, import_as in self._imports_)
        set_functions = '\n'.join('%s[%r] = function(%s) %s end' % (self._name_, func_name, func.lua_argdef, func.code) for func_name, func in self._functions_.iteritems())
        return '''
        do
            local {import_as_names}
            {own_import_name} = {own_name}
            {set_modules}
            {set_functions}
        end
        '''.format(
            import_as_names=import_as_names,
            set_modules=set_modules,
            set_functions=set_functions,
            own_name=module_map[self],
            own_import_name=self._name_
        )
    
    def _all_imports_recurse(self, s):
        for module, import_as in self._imports_:
            if module not in s:
                s.add(module)
                module._all_imports_recurse(s)
    
    def _all_imports(self):
        s = set()
        s.add(self)
        self._all_imports_recurse(s)
        return s
    
    def _compile_script(self):
        all_imports = self._all_imports()
        n = 0
        module_map = {}
        for module in all_imports:
            module_map[module] = '_LuaModule%s__%s' % (n, module._name_)
            n += 1
        initialize_modules = '\n'.join('local %s = {}' % module_name for module, module_name in module_map.iteritems())
        set_modules = '\n'.join(module._compile_module(module_map) for module in module_map)
        
        return '''
        {initialize_modules}
        {set_modules}
        do
            local name = ARGV[1]
            local argv = cjson.decode(ARGV[2])
            return cjson.encode({own_name}[name](unpack(argv)) or nil)
        end
        '''.format(
            initialize_modules=initialize_modules,
            set_modules=set_modules,
            own_name=module_map[self]
        )
    
    def _import_name_used(self, name):
        if name == self._name_:
            return True
        for module, import_as in self._imports_:
            if import_as == name:
                return True
        return False
    
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
            self._registered_script = redis.register_script(self._compile_script())
        args = json.dumps(args)
        if isinstance(redis, BasePipeline):
            # Messing with redis-py's internals a bit to support pipelines,
            # but I think in practice, this is the best way to do it.
            redis.set_response_callback('EVALSHA', _pipeline_response_callback)
            redis.evalsha = partial(redis.execute_command, 'EVALSHA', luamodule=True)
            self._registered_script(args=[name, args], client=redis)
            del redis.evalsha
            return redis
        else:
            return json.loads(self._registered_script(args=[name, args], client=redis))

    def __getattr__(self, name):
        if name.startswith('_') or name.endswith('_') or name not in self._functions_:
            raise AttributeError(name)
        return partial(self._call_, name)
