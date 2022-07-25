from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    needs_datetime_string_cast = True
    can_return_id_from_insert = False
    requires_rollback_on_dirty_transaction = True
    has_real_datatype = True
    can_defer_constraint_checks = True
    has_select_for_update = True
    has_select_for_update_nowait = True
    has_bulk_insert = False
    supports_tablespaces = False
    supports_transactions = True
    can_distinct_on_fields = False
    uses_autocommit = True
    uses_savepoints = False
    can_introspect_foreign_keys = False
    supports_timezones = False
