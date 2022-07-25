from datetime import datetime
from typing import NewType
from typing import Union, List, Iterable, Any

from django.db import models
from django.db.backends.base.operations import BaseDatabaseOperations

SQL = NewType('SQL', str)


# noinspection PyProtectedMember
# noinspection PyAbstractClass
# pylint: disable=abstract-method
class DatabaseOperations(BaseDatabaseOperations):
    def date_extract_sql(self, lookup_type: str, field_name: str) -> SQL:
        if lookup_type == 'week_day':
            # For consistency across backends, we return Sunday=1, Saturday=7.
            ret = 'MOD(WEEKDAY (%s) + 2,7)' % field_name
        else:
            ret = 'EXTRACT(%s FROM %s)' % (lookup_type, field_name)
        return ret

    def date_trunc_sql(self, lookup_type: str, field_name: str, tzname: str = None) -> SQL:
        # very low tech, code should be optimized
        ltypes = {'year': 'YYYY', 'month': 'YYYY-MM', 'day': 'YYYY-MM-DD'}
        cur_type = ltypes.get(lookup_type)
        if not cur_type:
            return field_name
        sql = "TO_DATE(TO_VARCHAR(%s, '%s'))" % (field_name, cur_type)
        return sql

    def datetime_trunc_sql(self, lookup_type: str, field_name: str, tzname: str) -> SQL:
        return self.date_trunc_sql(lookup_type, field_name)

    def datetime_cast_date_sql(self, field_name: str, tzname: str) -> SQL:
        return 'TO_DATE(%s)' % field_name

    def datetime_cast_time_sql(self, field_name: str, tzname: str) -> SQL:
        return 'TO_TIME(%s)' % field_name

    def datetime_extract_sql(self, lookup_type: str, field_name: str, tzname: str) -> SQL:
        # http://www.bestsaphanatraining.com/sap-hana-datetime-extract-function-examples.html
        return 'EXTRACT(%s FROM %s)' % (lookup_type, field_name)

    @staticmethod
    def value_to_db_datetime(value) -> Union[str, 'datetime']:
        """
        Transform a datetime value to an object compatible with what is expected
        by the backend driver for datetime columns.
        """
        if value is None:
            return None
        if value.tzinfo:
            # HANA doesn't support timezone. If tzinfo is present truncate it.
            # Better set USE_TZ=False in settings.py
            return datetime(value.year, value.month, value.day, value.hour,
                            value.minute, value.second, value.microsecond)
        return value

    def lookup_cast(self, lookup_type: str, internal_type: str = None) -> SQL:
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return 'UPPER(%s)'
        return '%s'

    def no_limit_value(self):
        return None

    def quote_name(self, name) -> str:
        return '"%s"' % name.replace('"', '""').upper()

    def sequence_reset_by_name_sql(self, style, sequences: Iterable) -> List[SQL]:
        sql = []
        for sequence_info in sequences:
            table_name = sequence_info['table']
            column_name = sequence_info['column']
            seq_name = self.get_seq_name(table_name, column_name)
            sql.append(
                'ALTER SEQUENCE %s RESET BY SELECT IFNULL(MAX(%s),0) + 1 from %s;' %
                (seq_name, column_name, table_name)
            )

        return sql

    def sequence_reset_sql(self, style, model_list: Iterable) -> List[SQL]:
        output = []
        qn = self.quote_name
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    output.append("%s %s %s %s %s %s" %
                                  (style.SQL_KEYWORD("ALTER SEQUENCE"),
                                   style.SQL_TABLE(self.get_seq_name(qn(model._meta.db_table), f.column)),
                                   style.SQL_KEYWORD("RESET BY SELECT"),
                                   style.SQL_FIELD('IFNULL(MAX(%s),0) + 1' % f.column),
                                   style.SQL_KEYWORD("FROM"),
                                   style.SQL_TABLE(qn(model._meta.db_table))))
                    break  # Only one AutoField is allowed per model, so don't bother continuing.
            for f in model._meta.many_to_many:
                if not f.rel.through:
                    output.append("%s %s %s %s %s %s" %
                                  (style.SQL_KEYWORD("ALTER SEQUENCE"),
                                   style.SQL_TABLE(self.get_seq_name(qn(f.m2m_db_table()), "id")),
                                   style.SQL_KEYWORD("RESET BY SELECT"),
                                   style.SQL_FIELD("IFNULL(MAX(id),0) + 1"),
                                   style.SQL_KEYWORD("FROM"),
                                   style.SQL_TABLE(qn(f.m2m_db_table()))))
        return output

    @staticmethod
    def prep_for_iexact_query(x):
        return x

    def max_name_length(self):
        """
            Returns the maximum length of table and column names, or None if there
            is no limit."""
        return 127

    def get_seq_name(self, table: str, column: str) -> str:
        return '%s_%s_%s_seq' % (self.connection.default_schema, table, column)

    def autoinc_sql(self, table: str, column: str) -> List[SQL]:
        seq_name = self.quote_name(self.get_seq_name(table, column))
        column = self.quote_name(column)
        table = self.quote_name(table)
        seq_sql = '''CREATE SEQUENCE %s RESET BY SELECT IFNULL(MAX(%s),0) + 1 FROM %s''' \
                  % (seq_name, column, table)
        return [seq_sql]

    def check_aggregate_support(self, aggregate) -> None:
        """Check that the backend supports the provided aggregate
        This is used on specific backends to rule out known aggregates
        that are known to have faulty implementations. If the named
        aggregate function has a known problem, the backend should
        raise NotImplementedError.
        """
        if aggregate.sql_function in ('STDDEV_POP', 'VAR_POP'):
            raise NotImplementedError()

    def start_transaction_sql(self):
        return ""

    def last_insert_id(self, cursor, table_name, pk_name):
        """
        Given a cursor object that has just performed an INSERT statement into
        a table that has an auto-incrementing ID, returns the newly created ID.
        This method also receives the table name and the name of the primary-key
        column.
        """
        cursor.execute('SELECT %s.currval FROM dummy' % self.connection.ops.get_seq_name(table_name, pk_name))
        return cursor.fetchone()[0]

    @staticmethod
    def convert_values(value, field) -> Union[Any, bool]:
        """
        Type conversion for boolean field. Keping values as 0/1 confuses
        the modelforms.
        """
        if field and field.get_internal_type() in ("BooleanField", "NullBooleanField") and value in (0, 1):
            value = bool(value)
        return value
