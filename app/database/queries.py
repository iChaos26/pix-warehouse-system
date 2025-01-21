from typing import Optional
from database.dtos import BaseDTO, CountryDTO, StateDTO, CityDTO, CustomerDTO, AccountDTO, TransferInDTO, TransferOutDTO, PixMovementDTO, MonthDTO, YearDTO, WeekDTO, WeekdayDTO, TimeDTO

class QueryBuilder:
    @staticmethod
    def insert_query(table_name: str, columns: list[str]) -> str:
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        return f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"

    @staticmethod
    def select_query(table_name: str, columns: list[str], conditions: Optional[str] = None) -> str:
        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        if conditions:
            query += f" WHERE {conditions}"
        return query + ";"

    @staticmethod
    def fetch_all(table_name: str) -> str:
        return f"SELECT * FROM {table_name};"

class Queries:
    @staticmethod
    def fetch_all_records(dto: BaseDTO) -> str:
        """Fetch all records for a given DTO's table."""
        return QueryBuilder.fetch_all(dto.table_name)

    @staticmethod
    def fetch_by_conditions(dto: BaseDTO, conditions: str) -> str:
        """Fetch records by conditions for a given DTO's table."""
        return QueryBuilder.select_query(dto.table_name, dto.columns, conditions)

    @staticmethod
    def insert_record(dto: BaseDTO) -> str:
        """Generate an insert query for a given DTO's table."""
        return QueryBuilder.insert_query(dto.table_name, dto.columns)

# # Example usage
# if __name__ == "__main__":
#     # Example: Fetch all records from the "country" table
#     print(Queries.fetch_all_records(CountryDTO()))

#     # Example: Fetch records with conditions
#     print(Queries.fetch_by_conditions(CountryDTO(), "country_id = '12345'"))

#     # Example: Generate insert query
#     print(Queries.insert_record(CountryDTO()))
