import pytest
from app.database.connection import DuckDBConnection
from app.database.queries import Queries
from app.database.update_dtos import PixMovementDTO, CountryDTO

@pytest.fixture(scope="module")
def db_connection():
    """
    Fixture to set up and tear down a DuckDB connection for testing.
    """
    connection = DuckDBConnection().connect()
    DatabaseSchema.create_core_schema(connection)
    DatabaseSchema.create_old_schema(connection)
    DatabaseSchema.create_new_schema(connection)
    yield connection
    connection.close()


def test_pix_movements_query(db_connection):
    """
    Tests the query generation and execution for the pix_movements table.
    """
    connection = db_connection

    # Insert mock data into pix_movements
    pix_movements_data = [
        ("uuid-1", "account-1", "in", 100.0, 1620000000, 1620000300, "completed"),
        ("uuid-2", "account-2", "out", 200.0, 1620001000, 1620001300, "failed"),
    ]
    insert_query = Queries.insert_record(PixMovementDTO())
    for row in pix_movements_data:
        connection.execute(insert_query, row)

    # Fetch all records from pix_movements
    fetch_query = Queries.fetch_all_records(PixMovementDTO())
    results = connection.execute(fetch_query).fetchall()

    # Assertions
    assert len(results) == 2
    assert results[0] == pix_movements_data[0]
    assert results[1] == pix_movements_data[1]
    print("Pix Movements Query Test Passed!")


def test_country_query(db_connection):
    """
    Tests the query generation and execution for the country table.
    """
    connection = db_connection

    # Insert mock data into country
    country_data = [("uuid-1", "USA"), ("uuid-2", "Canada")]
    insert_query = Queries.insert_record(CountryDTO())
    for row in country_data:
        connection.execute(insert_query, row)

    # Fetch by condition
    fetch_query = Queries.fetch_by_conditions(CountryDTO(), "country_id = 'uuid-1'")
    result = connection.execute(fetch_query).fetchall()

    # Assertions
    assert len(result) == 1
    assert result[0] == country_data[0]
    print("Country Query Test Passed!")
