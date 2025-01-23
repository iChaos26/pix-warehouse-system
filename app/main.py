import os
import glob
from app.database.connection import DuckDBConnection
from app.database.schema import DatabaseSchema
from app.transform.transform import DataTransformer
from app.database.queries import Queries
from app.database.update_dtos import PixMovementDTO, CountryDTO
from app.views.materialized_views import MaterializedViews
from app.views.transactions_views import TransactionsViews


class DataManager:
    """
    Manages the entire data lifecycle: schema creation, data loading,
    transformation, and materialized view creation.
    """

    def __init__(self, connection, csv_folder="data"):
        self.connection = connection
        self.csv_folder = csv_folder

    def load_csv_data(self):
        """
        Recursively loads CSV data from subfolders into the database.
        """
        print(f"Loading data from folder: {self.csv_folder}...")

        # Recursively find all CSV files in subfolders
        for root, _, files in os.walk(self.csv_folder):
            for file in files:
                if file.endswith(".csv"):
                    table_name = os.path.basename(root)  # Use folder name as the table name
                    csv_file_path = os.path.join(root, file)
                    print(f"Loading data from {csv_file_path} into {table_name}...")

                    # Create table and load data
                    self.connection.execute(
                        f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{csv_file_path}');"
                    )
                    print(f"Data from {csv_file_path} loaded successfully into {table_name}.")

    def perform_transformation(self):
        """
        Transforms data from the old schema to the new schema.
        """
        DataTransformer.transform_transactions(self.connection)

    def create_materialized_views(self):
        """
        Creates materialized views for reporting.
        """
        print("Creating materialized views...")
        MaterializedViews.create_monthly_account_balances(self.connection)
        MaterializedViews.create_daily_transactions_report(self.connection)
        TransactionsViews.create_customer_financial_overview(self.connection)
        TransactionsViews.create_top_performing_accounts(self.connection)
    
    def verify_data_in_tables(self, connection):
        tables = ["pix_movements", "country"]
        for table in tables:
            print(f"Verifying data in table: {table}")
            query = f"SELECT * FROM {table} LIMIT 5;"
            try:
                result = self.connection.execute(query).fetchall()
                print(f"Sample data in {table}: {result}")
            except Exception as e:
                print(f"Error verifying data in table {table}: {e}")

    def test_queries(self):
        """
        Tests queries using PixMovementDTO and CountryDTO.
        """

        try:
            print("Testing PixMovementDTO query...")
            # Fetch all records from pix_movements
            pix_movements_query = Queries.fetch_all_records(PixMovementDTO())
            pix_movements_data = self.connection.execute(pix_movements_query).fetchall()
            print("Pix Movements Data:", pix_movements_data)
            # Fetch country by conditions
            country_query = Queries.fetch_by_conditions(CountryDTO(), "country_id = '12345'")
            country_data = self.connection.execute(country_query).fetchall()
            print("Country Data:", country_data)

        except Exception as e:
            print(f"An error occurred during query testing: {e}")

def main():
    db_connection = DuckDBConnection()
    connection = db_connection.connect()

    try:

        # Step 1: Apply the old schema
        print("Applying old and core schema...")
        DatabaseSchema.create_core_schema(connection)
        DatabaseSchema.create_old_schema(connection)
        #Step 2: Apply the new schema
        print("Applying new schema...")
        DatabaseSchema.create_new_schema(connection)


        # Step 3: Load data into the old schema
        print("Loading data...")
        # Initialize DataManager
        manager = DataManager(connection)
        manager.load_csv_data()
        manager.verify_data_in_tables(connection)
        #print("Testing PixMovementDTO query...")
        #manager.test_queries()
        # Step 4: Transform old schema to new schema
        # print("Starting data transformation...")
        # print("Performing schema evolution...")
        # manager.perform_transformation()

        # # Step 5: Create materialized views
        # manager.create_materialized_views()

    except Exception as e:
        print(f"An error occurred: {str(e)}")

        print("Data pipeline execution complete!")
    finally:
        db_connection.close()


if __name__ == "__main__":
    main()
