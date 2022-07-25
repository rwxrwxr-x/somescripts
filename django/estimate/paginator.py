import re
import inspect

from django.db import connection
from django.core.paginator import Paginator
from django.utils.functional import cached_property
from django.utils.inspect import method_has_no_args


class LargeTablePaginator(Paginator):
    @cached_property
    def count(self):
        """Return the total number of objects, across all pages."""
        c = getattr(self.object_list, 'count', None)
        if callable(c) and not inspect.isbuiltin(c) and method_has_no_args(c):
            query_with_params = list(self.object_list.order_by().query.get_compiler(connection=connection).as_sql())
            query_with_params[0] = re.sub(r'^SELECT .+? FROM', 'SELECT 1 FROM', query_with_params[0])
            with connection.cursor() as cursor:
                cursor.execute(f'''SELECT count_estimate($${query_with_params[0]}$$)''', query_with_params[1])
                record = cursor.fetchone()
                if record[0] > self.per_page * 15:
                    return record[0]
                else:
                    return c()

        return len(self.object_list)
