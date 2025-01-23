from typing import Optional
from app.database.update_dtos import BaseDTO


class QueryBuilder:
    @staticmethod
    def insert_query(table_name: str, columns: list[str]) -> str:
        """
        Generates an INSERT query for a given table and columns.
        """
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        return f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"

    @staticmethod
    def select_query(table_name: str, columns: list[str], conditions: Optional[str] = None) -> str:
        """
        Generates a SELECT query for a given table and columns, optionally filtered by conditions.
        """
        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        if conditions:
            query += f" WHERE {conditions}"
        return query + ";"

    @staticmethod
    def fetch_all(table_name: str) -> str:
        """
        Generates a SELECT query to fetch all records from a table.
        """
        return f"SELECT * FROM {table_name};"


class Queries:
    @staticmethod
    def fetch_all_records(dto: BaseDTO) -> str:
        """
        Fetch all records for a given DTO's table.

        Args:
            dto (BaseDTO): The DTO representing the table.

        Returns:
            str: The generated SQL query.
        """
        if not dto.table_name or not dto.columns:
            raise ValueError("DTO must define both table_name and columns.")
        return QueryBuilder.fetch_all(dto.table_name)

    @staticmethod
    def fetch_by_conditions(dto: BaseDTO, conditions: str) -> str:
        """
        Fetch records by conditions for a given DTO's table.

        Args:
            dto (BaseDTO): The DTO representing the table.
            conditions (str): SQL WHERE conditions.

        Returns:
            str: The generated SQL query.
        """
        if not dto.table_name or not dto.columns:
            raise ValueError("DTO must define both table_name and columns.")
        return QueryBuilder.select_query(dto.table_name, dto.columns, conditions)

    @staticmethod
    def insert_record(dto: BaseDTO) -> str:
        """
        Generate an INSERT query for a given DTO's table.

        Args:
            dto (BaseDTO): The DTO representing the table.

        Returns:
            str: The generated SQL query.
        """
        if not dto.table_name or not dto.columns:
            raise ValueError("DTO must define both table_name and columns.")
        return QueryBuilder.insert_query(dto.table_name, dto.columns)
