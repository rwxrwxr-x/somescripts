from typing import Tuple


# pylint: disable=no-value-for-parameter

class Column(tuple):
    """Class for cursor.description representation"""

    _fields = (
        'name',
        'type_code',
        'display_size',
        'internal_size',
        'precision',
        'scale',
        'null_ok',
    )

    __slots__ = ()

    display_size = property(lambda self: object(), lambda self, v: None, lambda self: None)
    internal_size = property(lambda self: object(), lambda self, v: None, lambda self: None)
    name = property(lambda self: object(), lambda self, v: None, lambda self: None)
    null_ok = property(lambda self: object(), lambda self, v: None, lambda self: None)
    precision = property(lambda self: object(), lambda self, v: None, lambda self: None)
    scale = property(lambda self: object(), lambda self, v: None, lambda self: None)
    type_code = property(lambda self: object(), lambda self, v: None, lambda self: None)

    def _asdict(self) -> dict:
        return {f: getattr(self, f) for f in self._fields}

    @classmethod
    def _make(cls, *args) -> Tuple['Column']:
        return tuple(cls(**dict(zip(cls._fields, x))) for x in args)

    def _replace(self, **kwargs) -> 'Column':
        params = self._asdict()
        params.update(kwargs)
        return self.__class__(params)

    def __getnewargs__(self):
        return tuple(self)

    def __new__(cls, name, type_code, display_size, internal_size, precision, scale, null_ok):
        return tuple.__new__(cls, (name, type_code, display_size, internal_size, precision, scale, null_ok))

    def __init__(self, name, type_code, display_size,  # pylint: disable=super-init-not-called
                 internal_size, precision, scale, null_ok):
        self.name = name
        self.type_code = type_code
        self.display_size = display_size
        self.internal_size = internal_size
        self.precision = precision
        self.scale = scale
        self.null_ok = null_ok

    def __repr__(self):
        return '%(cls)s(' \
               '%(name)r, %(type_code)r, %(display_size)r,' \
               ' %(internal_size)r, %(precision)r, %(scale)r, %(null_ok)r)' % {
                   'cls': self.__class__.__name__,
                   'name': self.name,
                   'type_code': self.type_code,
                   'display_size': self.display_size,
                   'internal_size': self.internal_size,
                   'precision': self.precision,
                   'scale': self.scale,
                   'null_ok': self.null_ok,
               }
