import duckdb
from constants import DUCKDB_FILE
from decorators import log
from polars import DataFrame
from contextlib import contextmanager


class Database:
    def connect(self):
        log("Connecting to database")
        self.conn = duckdb.connect(DUCKDB_FILE, read_only=True)
        self.cursor = self.conn.cursor()

    def query(self, q: str, params: tuple = None) -> DataFrame:
        log("Executing query")
        print(f"Executing query: {q}")
        if params:
            print(f"Params: {params}")
            result = self.cursor.execute(q, params).pl()
        else:
            print(f"No params")
            result = self.cursor.execute(q).pl()
        return result

    def close(self):
        log("Closing database connection")
        self.conn.close()

    def __enter__(self):
        log("Enter the database context")
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        log("Database connection closed")
        self.close()


@contextmanager
def managed_db():
    db = Database()
    db.connect()
    yield db
    db.close()
