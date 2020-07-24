import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.exceptions import (
    DatabaseException,
    FailedToConnectException,
    InternalException,
    RuntimeException,
    warn_or_error,
)
from dbt.logger import GLOBAL_LOGGER as logger


@dataclass
class SQLiteCredentials(Credentials):
    """ Required connections for a SQLite connection"""

    host: str
    schemas: str

    @property
    def type(self):
        return "sqlite"

    def _connection_keys(self):
        """ Keys to show when debugging """
        return ["host", "schemas"]


class SQLiteConnectionManager(SQLConnectionManager):
    TYPE = "sqlite"

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        schemas = connection.credentials.schemas.split(',')

        try:
            handle: sqlite3.Connection = sqlite3.connect(host=credentials.host)
            cursor = handle.cursor()
            for schema in schemas:
                schema_file, schema_name = schema, schema.replace('.db', '')
                cursor.execute(f"attach '{schema_file}' as {schema_name}")
            connection.state = "open"
            connection.handle = handle

            return connection
        except sqlite3.Error as e:
            logger.debug(
                "Got an error when attempting to open a sqlite3 connection: '%s'", e
            )
            connection.handle = None
            connection.state = "fail"

            raise FailedToConnectException(str(e))

    @classmethod
    def get_status(cls, cursor: sqlite3.Cursor):
        return f"OK {cursor.rowcount}"

    def cancel(self, connection):
        """ cancel ongoing queries """

        logger.debug("Cancelling queries")
        try:
            connection.handle.interrupt()
        except sqlite3.Error:
            pass
        logger.debug("Queries canceled")

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except sqlite3.DatabaseError as e:
            self.release()
            logger.debug("sqlite3 error: {}".format(str(e)))
            raise DatabaseException(str(e))
        except Exception as e:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            self.release()
            raise RuntimeException(str(e))
