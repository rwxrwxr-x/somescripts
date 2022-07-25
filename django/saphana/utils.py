import sys
from time import time

from django.db import utils
from .extras import IntegrityError, Error


class CursorWrapper:
    """
        Hana doesn't support %s placeholders
        Wrapper to convert all %s placeholders to qmark(?) placeholders
    """
    codes_for_integrityerror = (301,)

    def __init__(self, cursor, db):
        self.cursor = cursor
        self.db = db
        self.is_hana = True

    def set_dirty(self):
        if not self.db.get_autocommit():
            self.db.set_dirty()

    def __getattr__(self, attr):
        self.set_dirty()
        if attr in self.__dict__:
            ret = self.__dict__[attr]
        else:
            ret = getattr(self.cursor, attr)
        return ret

    def __iter__(self):
        return iter(self.cursor)

    def _integrity_error_handler(self, exception, info):
        """
        Map some error codes to IntegrityError, since they seem to be misclassified and Django would prefer
         the more logical place.
        :param exception:
        :param info:
        :return:
        """
        if exception[0] in self.codes_for_integrityerror:
            raise utils.IntegrityError(info[2]) from exception

    def execute(self, sql, params=()):
        """
        Execute a query.
        :param sql:
        :param params:
        :return:
        """
        try:
            self.cursor.execute(self._replace_params(sql, len(params) if params else 0), params)
        except (IntegrityError, Error) as exc:
            self._integrity_error_handler(exc, sys.exc_info())

    def executemany(self, sql, param_list):
        """
        Execute a multi-row query.
        :param sql:
        :param param_list:
        :return:
        """
        try:
            self.cursor.executemany(
                self._replace_params(sql, len(param_list[0]) if param_list and len(param_list) > 0 else 0), param_list)
        except (IntegrityError, Error) as exc:
            self._integrity_error_handler(exc, sys.exc_info())

    @staticmethod
    def _replace_params(sql, params_count):
        """
        Replace all %s placeholders with qmark(?) placeholders
        :param sql:
        :param params_count:
        :return:
        """
        return sql % tuple('?' * params_count)


class CursorDebugWrapper(CursorWrapper):

    def execute(self, sql, params=()):
        """
        Execute a query.
        :param sql:
        :param params:
        :return:
        """
        self.set_dirty()
        start = time()
        try:
            return CursorWrapper.execute(self, sql, params)
        finally:
            stop = time()
            duration = stop - start
            sql = self.db.ops.last_executed_query(self.cursor, sql, params)
            self.db.queries.append({
                'sql': sql,
                'time': "%.3f" % duration,
            })

    def executemany(self, sql, param_list):
        """
        Execute a multi-row query.
        :param sql:
        :param param_list:
        :return:
        """
        self.set_dirty()
        start = time()
        try:
            return CursorWrapper.executemany(self, sql, param_list)
        finally:
            stop = time()
            duration = stop - start
            try:
                times = len(param_list)
            except TypeError:  # param_list could be an iterator
                times = '?'
            self.db.queries.append({
                'sql': '%s times: %s' % (times, sql),
                'time': "%.3f" % duration,
            })
