import duckdb


class DuckDBConnection:
    def __init__(self, database_path: str = ":memory:"):
        self.database_path = database_path
        self.connection = None

    def connect(self):
        if not self.connection:
            self.connection = duckdb.connect(self.database_path)
        return self.connection

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None
