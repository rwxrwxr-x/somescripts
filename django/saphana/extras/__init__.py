from typing import Tuple, Union, Any, Iterable, Iterator, Generator, List

from .types import Column

try:
    from hdbcli import dbapi
except ImportError as e:
    raise ImportError("Error loading SAP HANA Python driver: %s" % e) from e

IntegrityError = dbapi.IntegrityError
Error = dbapi.Error
_cursor = dbapi.Cursor
_connection = dbapi.Connection

__all__ = [
    'IntegrityError',
    'Error',
    'Cursor',
    'BaseCursor',
    'DictCursor',
    'RealDictCursor',
    'Connection'
]


# pylint: disable=too-many-public-methods
# noinspection PyProtectedMember
class Cursor(_cursor):
    """
    Proxy class for hdbcli cursor
    """

    def __init__(self, cursor: '_cursor'):
        self._cursor = cursor

    @property
    def description(self) -> Tuple[Column]:
        return Column._make(*self._cursor.description)

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def server_cpu_time(self) -> int:
        """
        Returns the CPU time used by the server.
        :return:
        """
        return self._cursor.server_cpu_time()

    @property
    def server_memory_usage(self) -> int:
        return self._cursor.server_memory_usage

    @property
    def server_processing_time(self) -> int:
        return self._cursor.server_processing_time

    @property
    def connection(self) -> 'Connection':
        return self._cursor.connection

    def callproc(self, *args, **kwargs) -> tuple:
        """
        Returns a tuple of all of the parameters.
        Input values are returned unchanged,
        while output and input/output parameters may be replaced with new values.
        If the procedure provides a result set as output,
        then make the result available through the standard fetch methods.
        :param args: procname string, tuple parameters
        :param kwargs: overview boolean
        :return:
        """
        return self._cursor.callproc(*args, **kwargs)

    def clearwarning(self) -> None:
        return self._cursor.clearwarning()

    def close(self) -> None:
        return self._cursor.close()

    def description_ext(self) -> Tuple[str]:
        return self._cursor.description_ext()

    def execute(self, query: str, variables) -> bool:
        """
        Executes a query.
        :param query:
        :param variables:
        :return:
        """
        return self._cursor.execute(query, variables)

    def executemany(self, query: str, args) -> bool:
        return self._cursor.executemany(query, args)

    def executemanyprepared(self, query: str, args) -> bool:
        return self._cursor.executemanyprepared(query, args)

    def executeprepared(self, query: str, *args) -> bool:
        return self._cursor.executeprepared(query, *args)

    def fetchall(self) -> Tuple[tuple]:
        return self._cursor.fetchall()

    def fetchmany(self, size=None) -> Tuple[tuple]:
        return self._cursor.fetchmany(size)

    def fetchone(self) -> tuple:
        return self._cursor.fetchone()

    @property
    def resultset_holdability(self) -> int:
        """
        Returns the result set holdability.
        :return:
        """
        return self._cursor.get_resultset_holdability()

    @property
    def packetsize(self) -> int:
        """
        Returns the size of the packets.
        :return:
        """
        return self._cursor.getpacketsize()

    @property
    def warning(self) -> str:
        """
        Returns the warning.
        :return:
        """
        return self._cursor.getwarning()

    @property
    def has_result_set(self) -> bool:
        """
        Returns whether the cursor has a result set.
        :return:
        """
        return self._cursor.has_result_set()

    @property
    def haswarning(self) -> bool:
        """
        Returns whether the cursor has a warning.
        :return:
        """
        return self._cursor.haswarning()

    @property
    def parameter_description(self):
        """
        Retrieves a sequence of parameter descriptions.
        :return:
        """
        return self._cursor.parameter_description()

    def prepare(self, query: str) -> bool:
        """
        Prepares a query for execution.
        :param query:
        :return:
        """
        return self._cursor.prepare(query)

    def set_resultset_holdability(self, holdability: int) -> None:
        """
        Specifies whether the cursor for a statement is held or rolled back after the transaction is committed.
         A held cursor is still valid after a commit or rollback, and the application is able to
          fetch more rows or result sets from it.

        Holding cursors, particularly hold over rollback, causes the SAP HANA server to use more resources.

        Support for holding a cursor over rollback or over commit and rollback is available in
         SAP HANA servers beginning with SAP HANA 2.0 SPS 04.

        0 CURSOR_HOLD_OVER_COMMIT
        1 CURSOR_CLOSE_ON_COMMIT_OR_ROLLBACK
        2 CURSOR_HOLD_OVER_ROLLBACK
        This value is only supported for SAP HANA.
        3 CURSOR_HOLD_OVER_ROLLBACK_AND_COMMIT
        This value is only supported for SAP HANA.
        :param holdability:
        :return:
        """
        return self._cursor.set_resultset_holdability(holdability)

    def setfetchsize(self, size) -> None:
        """
        Sets the size of prefetched result sets.
        :param size:
        :return:
        """
        return self._cursor.setfetchsize(size)

    def setinputsizes(self, sizes) -> None:
        """
        Sets the size of the input parameters.
        :param sizes:
        :return:
        """
        return self._cursor.setinputsizes(sizes)

    def setoutputsize(self, size, column=None) -> None:
        """
        Sets the size of the output parameter.
        :param size:
        :param column:
        :return:
        """
        return self._cursor.setoutputsize(size, column)

    def setpacketsize(self, size) -> None:
        """
        Sets the size of the packets.
        :param size:
        :return:
        """
        return self._cursor.setpacketsize(size)

    def setquerytimeout(self, seconds) -> None:
        """
        Sets the query timeout.
        :param seconds:
        :return:
        """
        return self._cursor.setquerytimeout(seconds)


# pylint: enable=too-many-public-methods

############################
# Base Cursor and some stuff
############################

class BaseCursor(Cursor):
    """Base class for all dict-like cursors."""

    def __init__(self, cursor, **kwargs):  # pylint: disable=super-init-not-called
        self._row_factory = kwargs.get('row_factory', BaseRow)
        self._query_executed = 0
        self._prefetch = 0
        self._cursor = cursor

    def fetchone(self) -> 'BaseRow':
        self._build_index()
        res = self._cursor.fetchone()

        if not isinstance(res, (tuple, list)):
            res = tuple(res)
        return self._row_factory(self).pack(res)

    def fetchmany(self, size=None) -> List['BaseRow']:
        self._build_index()
        res = self._cursor.fetchmany(size)
        return [self._row_factory(self).pack(x) for x in res]

    def fetchall(self) -> List['BaseRow']:
        self._build_index()
        res = self._cursor.fetchall()
        return [self._row_factory(self).pack(x) for x in res]

    def __iter__(self):
        try:
            res = self._cursor.__iter__()
            first = next(res)

            yield first
            while 1:
                yield next(res)
        except StopIteration:
            return


class BaseRow:
    def pack(self, args: Iterable) -> 'BaseRow':
        if args:
            for i, v in enumerate(args):
                self[i] = v
        return self


############################
# Dict Cursor and some stuff
############################

class DictCursor(BaseCursor):
    def __init__(self, cursor, **kwargs):
        kwargs['row_factory'] = DictRow
        self.index = {}
        self._prefetch = 1
        super(DictCursor, self).__init__(cursor, **kwargs)

    def execute(self, query: str, variables: Union[tuple, dict] = None) -> bool:
        self.index = {}
        res = self._cursor.execute(query, variables)
        self._query_executed = 1
        return res

    def callproc(self, proc_name: str, variable: Union[tuple, dict] = None) -> bool:  # pylint: disable=arguments-differ
        self.index = {}
        res = self._cursor.callproc(proc_name, variable)
        self._query_executed = 1
        return res

    def _build_index(self) -> None:
        if self._query_executed == 1 and self.description:
            for i in range(len(self.description)):
                self.index[self.description[i][0]] = i
            self._query_executed = 0


class DictRow(list, BaseRow):
    """A row object that allow by-column-name access to data."""

    __slots__ = ('_index',)

    def __init__(self, cursor: 'DictCursor') -> None:  # pylint: disable=super-init-not-called
        self._index = cursor.index
        self[:] = [None] * len(cursor.description)

    def __getitem__(self, x: Union[Any, int, slice]) -> Any:
        if not isinstance(x, (int, slice)):
            x = self._index[x]
        return list.__getitem__(self, x)

    def __setitem__(self, x: Union[Any, int, slice], v: Any) -> None:
        if not isinstance(x, (int, slice)):
            x = self._index[x]
        list.__setitem__(self, x, v)

    def items(self) -> Tuple[Any, Any]:
        return zip(self.iteritems(), self.values())

    def keys(self) -> list:
        return list(self._index.keys())

    def values(self) -> list:
        return tuple(self[:])

    def has_key(self, x) -> bool:
        return x in self._index

    def get(self, x, default=None) -> Any:
        try:
            return self[x]
        except Exception:
            return default

    def iteritems(self) -> Generator:
        for n, v in self._index.items():
            yield n, list.__getitem__(self, v)

    def iterkeys(self) -> list:
        return iter(self._index.keys())

    def itervalues(self) -> Iterator:
        return list.__iter__(self)

    def copy(self) -> Any:
        return dict(iter(self.items()))

    def __contains__(self, x):
        return x in self._index

    def __getstate__(self):
        return self[:], self._index.copy()

    def __setstate__(self, data):
        self[:] = data[0]
        self._index = data[1]

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, list.__repr__(self))


############################
# Real Dict Cursor and some stuff
############################

class RealDictCursor(DictCursor):
    def __init__(self, cursor, **kwargs):
        kwargs['row_factory'] = RealDictRow
        super(DictCursor, self).__init__(cursor, **kwargs)  # pylint: disable=bad-super-call
        self._prefetch = 0
        self.column_mapping = []

    def execute(self, query: str, variables: Union[tuple, dict] = None) -> bool:  # pylint: disable=arguments-differ
        self.column_mapping = []
        res = self._cursor.execute(query, variables)
        self._query_executed = 1
        return res

    def callproc(self, proc_name: str, variable: Union[tuple, dict] = None) -> bool:
        self.column_mapping = []
        res = self._cursor.callproc(proc_name, variable)
        self._query_executed = 1
        return res

    def _build_index(self):
        if self._query_executed == 1 and self.description:
            for i in range(len(self.description)):
                self.column_mapping.append(self.description[i][0].lower())
            self._query_executed = 0


# noinspection PyProtectedMember
class RealDictRow(dict, BaseRow):
    """A `!dict` subclass representing a data record."""

    __slots__ = ('_column_mapping',)

    def __init__(self, cursor: 'RealDictCursor'):
        dict.__init__(self)
        # Required for named cursors
        if cursor.description and not cursor.column_mapping:
            cursor._build_index()

        self._column_mapping = cursor.column_mapping

    def __setitem__(self, name: Any, value: Any):
        if isinstance(name, int):
            name = self._column_mapping[name]
        return dict.__setitem__(self, name, value)

    def __getstate__(self):
        return self.copy(), self._column_mapping[:]

    def __setstate__(self, data):
        self.update(data[0])
        self._column_mapping = data[1]

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, dict.__repr__(self))


class Connection(_connection):
    def cursor(self, *args, cursor_factory=None, **kwargs) -> Union['_cursor', 'DictCursor', 'RealDictCursor']:
        """
        Creates cursor with cursor_factory
        :param args:
        :param cursor_factory:
        :param kwargs:
        :return:
        """
        cursor = super().cursor()
        if cursor_factory:
            return cursor_factory(cursor)
        return cursor
