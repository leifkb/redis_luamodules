from functools import partial
from inspect import isclass, getargspec
try:
    import ujson as json
except:
    import json

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
            yield method.__name__, (argspec.args, method.__doc__)
    
    def _compile_module(self, module_map):
        self._compile_called = True
        import_as_names = [import_as for module, import_as in self._imports_]
        import_as_names.append(self._name_)
        import_as_names = ', '.join(import_as_names)
        set_modules = '\n'.join('%s = %s' % (import_as, module_map[module]) for module, import_as in self._imports_)
        set_functions = '\n'.join('%s[%r] = function(%s) %s end' % (self._name_, function_name, ', '.join(args), code) for function_name, (args, code) in self._functions_.iteritems())
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
        set_modules = '\n'.join('%s' % (module._compile_module(module_map)) for module, module_name in module_map.iteritems())
        
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
        if redis is None and self._redis_ is None:
            raise TypeError('No redis client specified calling LuaModule, which was created without a default redis client.')
        redis = redis or self._redis_
        if self._registered_script is None:
            self._registered_script = redis.register_script(self._compile_script())
        args = json.dumps(args)
        return json.loads(self._registered_script(args=[name, args], client=redis))

    def __getattr__(self, name):
        if name.startswith('_') or name.endswith('_') or name not in self._functions_:
            raise AttributeError(name)
        return partial(self._call_, name)
