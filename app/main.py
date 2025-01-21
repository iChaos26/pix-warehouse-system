from database.connection import DuckDBConnection
from database.schema import DatabaseSchema
from schema_evolution import SchemaEvolution
from views.materialized_views import MaterializedViews
from views.transactions_views import TransactionsViews


def main():
    db_connection = DuckDBConnection()
    connection = db_connection.connect()

    try:
        # Step 1: Apply the old schema
        print("Applying old schema...")
        DatabaseSchema.create_old_schema(connection)

        # Step 2: Load data into the old schema
        print("Loading data into old schema...")
        load_and_transform_data_with_duckdb(connection, csv_folder="data")

        # Step 3: Transform old schema to new schema
        print("Evolving schema to new format...")
        SchemaEvolution.transform_to_new_schema(connection)

        # Step 4: Apply the new schema
        print("Applying new schema...")
        DatabaseSchema.create_new_schema(connection)

        # Step 5: Create materialized views
        print("Creating materialized views...")
        MaterializedViews.create_monthly_account_balances(connection)
        MaterializedViews.create_daily_transactions_report(connection)
        TransactionsViews.create_customer_financial_overview(connection)
        TransactionsViews.create_top_performing_accounts(connection)

        print("Database setup complete!")
    finally:
        db_connection.close()


if __name__ == "__main__":
    main()
