from functools import partial
from inspect import isclass, getargspec

class Scripts(object):
    def __new__(cls, *args):
        if len(args) == 1 and isclass(args[0]):
            return Scripts(None, args[0])
        elif len(args) == 1: # redis
            return partial(Scripts, args[0])
        else:
            redis, scripts = args
            self = object.__new__(cls)
            if isclass(scripts):
                scripts = self._extract_lua_from_class(scripts)
            self._scripts_ = dict(scripts)
            compiled_script = self._compile_script(self._scripts_)
            if redis is not None:
                self._registered_script = redis.register_script(compiled_script)
            else:
                self._compiled_script = compiled_script
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

    def _compile_script(self, scripts):
        table_entries = []
        for name, (args, code) in scripts.iteritems():
            function = 'function(%s) %s end' % (', '.join(args), code)
            table_entry = '[%r] = %s' % (name, function)
            table_entries.append(table_entry)
        table = '{' + ', '.join(table_entries) + '}'
        return '''
            local S
            S = %s
            do
                local name = ARGV[1]
                local argv = cjson.decode(ARGV[2])
                return cjson.encode(S[name](unpack(argv)))
            end
        ''' % table

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
