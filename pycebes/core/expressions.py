from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals


from collections import namedtuple


_ParamConfig = namedtuple('_ParamConfig', ['name', 'param_type', 'server_name'])


def param(name='param', param_type='int', server_name=None):
    def decorate(cls):
        assert issubclass(cls, Expression)
        if getattr(cls, 'PARAMS', None) is None:
            setattr(cls, 'PARAMS', [])
        cls.PARAMS.append(_ParamConfig(name, param_type, name if server_name is None else server_name))
        return cls
    return decorate


class Expression(object):

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def to_json(self):
        js = {'exprType': self.__class__.__name__}
        for pc in getattr(self, 'PARAMS', []):
            assert isinstance(pc, _ParamConfig)
            js[pc.server_name] = Expression._to_json(getattr(self, pc.name, None))
        return js

    @classmethod
    def _to_json(cls, v):
        if v is None:
            return v
        if isinstance(v, Expression):
            return v.to_json()
        if isinstance(v, (list, tuple)):
            return [cls._to_json(vv) for vv in v]

        raise ValueError('Unsupported')

"""
Arithmetic
"""


@param('child')
class Abs(Expression):
    def __init__(self, child):
        super(Abs, self).__init__(child=child)

