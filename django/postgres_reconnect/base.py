from django.db.backends.postgresql.base import DatabaseWrapper as PostgresWrapper


class DatabaseWrapper(PostgresWrapper):
    def ensure_connection(self):
        if self.connection is None:
            with self.wrap_database_errors:
                self.connect()
        elif self.connection.closed:
            with self.wrap_database_errors:
                self.connect()
