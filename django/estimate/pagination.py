from collections import OrderedDict

from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response

from .paginator import LargeTablePaginator


class LargeTableStandardResultsSetPagination(PageNumberPagination):
    django_paginator_class = LargeTablePaginator
    page_size_query_param = 'limit'
    max_page_size = 100
    page_size = 15

    def get_paginated_response(self, data, response=True):
        res = OrderedDict([
            ('count', self.page.paginator.count),
            ('next', self.get_next_link()),
            ('previous', self.get_previous_link()),
            ('current_page', self.page.number),
            ('total_pages', self.page.paginator.num_pages),
            ('limit', self.page.paginator.per_page),
            ('results', data),
        ])
        if response:
            return Response(res)
        else:
            return res
