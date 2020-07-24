from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sqlite import SQLiteConnectionManager


class SQLiteAdapter(SQLAdapter):
    ConnectionManager = SQLiteConnectionManager

    @classmethod
    def date_function(cls):
        return 'datetime()'