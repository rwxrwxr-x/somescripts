"""
SAP HANA database backend for Django.
"""

from contextlib import contextmanager
from typing import Generator, Union, Any

from django.core import exceptions
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.base.validation import BaseDatabaseValidation
from django.utils.functional import cached_property

from saphana.client import DatabaseClient
from saphana.extras import Connection, Cursor

from saphana.features import DatabaseFeatures
from saphana.introspection import DatabaseIntrospection
from saphana.operations import DatabaseOperations
from saphana.utils import CursorDebugWrapper, CursorWrapper, Error as DatabaseError

try:
    from hdbcli import dbapi as database
except ImportError as e:
    raise exceptions.ImproperlyConfigured("Error loading SAP HANA Python driver: %s" % e) from e


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'hana'
    display_name = 'SAP HANA'
    default_schema = None
    _cursor_factory = None

    Database = database
    SchemaEditorClass = BaseDatabaseSchemaEditor
    client_class = DatabaseClient
    creation_class = BaseDatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    validation_class = BaseDatabaseValidation
    ops_class = DatabaseOperations

    data_types = {
        'AutoField': 'INTEGER',
        'BigIntegerField': 'BIGINT',
        'BinaryField': 'BLOB',
        'BooleanField': 'TINYINT',
        'CharField': 'NVARCHAR(%(max_length)s)',
        'DateField': 'DATE',
        'DateTimeField': 'TIMESTAMP',
        'DecimalField': 'DECIMAL(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'BIGINT',
        'FileField': 'NVARCHAR(%(max_length)s)',
        'FilePathField': 'NVARCHAR(%(max_length)s)',
        'FloatField': 'FLOAT',
        'GenericIPAddressField': 'NVARCHAR(39)',
        'ImageField': 'NVARCHAR(%(max_length)s)',
        'IntegerField': 'INTEGER',
        'NullBooleanField': 'TINYINT',
        'OneToOneField': 'INTEGER',
        'PositiveIntegerField': 'INTEGER',
        'PositiveSmallIntegerField': 'SMALLINT',
        'SlugField': 'NVARCHAR(%(max_length)s)',
        'SmallIntegerField': 'SMALLINT',
        'TextField': 'NCLOB',
        'TimeField': 'TIME',
        'URLField': 'NVARCHAR(%(max_length)s)',
        'UUIDField': 'NVARCHAR(32)',
    }

    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }

    def get_connection_params(self) -> dict:
        conn_params = {
            k.lower(): v for k, v in self.settings_dict.items()
            if k in ('USER', 'PASSWORD', 'HOST', 'PORT')
        }

        conn_params['address'] = conn_params.get('host')
        if not conn_params.get('name'):
            conn_params['name'] = 'SAPHANADB'

        if conn_params.get('name', '') == '':
            raise exceptions.ImproperlyConfigured('You must specify a database name in your settings')

        if len(conn_params.get('name')) > self.ops.max_name_length():
            raise exceptions.ImproperlyConfigured(
                "The database name '%s' (%d characters) is longer than "
                "HANA's limit of %d characters. Supply a shorter NAME "
                "in settings.DATABASES." % (
                    conn_params['name'],
                    len(conn_params['name']),
                    self.ops.max_name_length(),
                )
            )
        for option, value in self.settings_dict.get('OPTIONS', {}).items():
            conn_params[option] = value
        self.default_schema = conn_params.pop('name', '').upper()

        return conn_params

    def get_new_connection(self, conn_params: dict) -> Connection:
        return Connection(cursor_factory=self.cursor_factory, **conn_params)

    def init_connection_state(self) -> None:
        """
        Initialize the database connection to a known state.
        :return:
        """
        cursor = self.cursor()
        cursor.execute('SELECT (1) AS a FROM SCHEMAS WHERE SCHEMA_NAME=\'%s\'' % self.default_schema)
        res = cursor.fetchone()
        if not res:
            cursor.execute('create schema %s' % self.default_schema)
        cursor.execute('set schema ' + self.default_schema)

    def create_cursor(self, name: str = None) -> Cursor:
        """
        inherited signature
        :param name: unused
        :return:
        """
        return self.connection.cursor()

    @property
    def cursor_factory(self) -> Cursor:
        return self._cursor_factory

    @cursor_factory.setter
    def cursor_factory(self, cursor_class: Union[type, str] = None) -> None:
        self._cursor_factory = cursor_class or Cursor

    def _set_autocommit(self, autocommit: bool) -> None:
        self.connection.setautocommit(autocommit)

    def is_usable(self) -> bool:
        try:
            self.connection.cursor().execute('SELECT 1 FROM dummy')
        except DatabaseError:
            return False
        else:
            return True

    @cached_property
    def hana_version(self) -> str:
        with self.temporary_connection() as cursor:
            cursor.execute('SELECT version FROM sys.m_database')
            return next(iter(cursor.fetchone()))

    @contextmanager
    def temporary_connection(self) -> Generator[Union[CursorDebugWrapper, CursorWrapper], Any, None]:
        """
        Context manager that ensures that a connection is established, and
        if it opened one, closes it to avoid leaving a dangling connection.
        This is useful for operations outside of the request-response cycle.

        Provide a cursor: with self.temporary_connection() as cursor: ...
        """
        must_close = self.connection is None

        try:
            yield self.cursor()
        finally:
            if must_close:
                self.close()

    def _prepare_cursor(self, cursor) -> Union[CursorDebugWrapper, CursorWrapper]:
        """
        Validate the connection is usable and perform database cursor wrapping.
        """
        self.validate_thread_sharing()
        if self.queries_logged:
            wrapped_cursor = self.make_debug_cursor(cursor)
        else:
            wrapped_cursor = self.make_cursor(cursor)
        return wrapped_cursor

    def make_cursor(self, cursor) -> CursorWrapper:
        return CursorWrapper(cursor, self)

    def make_debug_cursor(self, cursor) -> CursorDebugWrapper:
        return CursorDebugWrapper(cursor, self)

    def raw_cursor(self, *args, **kwargs):
        if not self.connection:
            self.ensure_connection()
        return self.connection.cursor(*args, **kwargs)
