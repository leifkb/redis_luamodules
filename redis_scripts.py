from functools import partial
from inspect import isclass, getargspec
try:
    import ujson as json
except:
    import json

class Scripts(object):
    def __new__(cls, *args, **kwargs):
        if len(args) == 0 and kwargs:
            return partial(Scripts, **kwargs)
        elif len(args) == 1 and isclass(args[0]):
            return Scripts(None, args[0], **kwargs)
        elif len(args) == 1: # redis
            return partial(Scripts, args[0], **kwargs)
        else:
            if len(args) == 2:
                redis, scripts = args
                table_name = None
            elif len(args) == 3:
                redis, scripts, table_name = args
            else:
                raise TypeError('Wrong number of args')
            if table_name is None:
                table_name = kwargs.pop('table_name', None)
            if kwargs:
                raise TypeError('Excess kwargs: %s' % kwargs)
            if table_name is None:
                table_name = getattr(scripts, '__name__', 'S')
            
            self = object.__new__(cls)
            if isclass(scripts):
                scripts = self._extract_lua_from_class(scripts)
            self._scripts_ = dict(scripts)
            self._compiled_script = self._compile_script(table_name, self._scripts_)
            if redis is not None:
                self._registered_script = redis.register_script(self._compiled_script)
            else:
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

    def _compile_script(self, table_name, scripts):
        table_entries = []
        for name, (args, code) in scripts.iteritems():
            function = 'function(%s) %s end' % (', '.join(args), code)
            table_entry = '[%r] = %s' % (name, function)
            table_entries.append(table_entry)
        table = '{' + ', '.join(table_entries) + '}'
        return '''
            local {table_name}
            {table_name} = {table}
            do
                local name = ARGV[1]
                local argv = cjson.decode(ARGV[2])
                return cjson.encode({table_name}[name](unpack(argv)) or nil)
            end
        '''.format(table=table, table_name=table_name)

    def _call_(self, name, *args, **kwargs):
        redis = kwargs.pop('redis', None)
        if kwargs:
            raise TypeError('Excess kwargs: %s' % kwargs.keys())
        if redis is None and self._redis_ is None:
            raise TypeError('No redis client specified calling Scripts object, which was created without a default redis client.')
        if self._registered_script is None:
            self._registered_script = redis.register_script(self._compiled_script)
        args = json.dumps(args)
        return json.loads(self._registered_script(args=[name, args], client=redis))

    def __getattr__(self, name):
        if name.startswith('_') or name.endswith('_') or name not in self._scripts_:
            raise AttributeError(name)
        return partial(self._call_, name)
