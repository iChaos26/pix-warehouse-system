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
                    print(csv_file_path)
                    self.connection.sql(
                        f"CREATE TABLE {table_name} AS SELECT * FROM read_csv('{csv_file_path}', AUTO_DETECT=TRUE)"
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
        #DatabaseSchema.create_core_schema(connection)
        #DatabaseSchema.create_old_schema(connection)
        #Step 2: Apply the new schema
        print("Applying new schema...")
        #DatabaseSchema.create_new_schema(connection)


        # Step 3: Load data into the old schema
        print("Loading data...")
        # Initialize DataManager
        manager = DataManager(connection)
        manager.load_csv_data()
        manager.verify_data_in_tables(connection)
        #connection.table("pix_movements").show()
        print("Testing PixMovementDTO and CountryDTO query builder...")
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
#OUTPUT
# Verifying data in table: pix_movements
# Sample data in pix_movements: [(3101512580483427328, 127700825787235152, 1.56, 1587921566, 'None', 'failed', 'pix_in'), (2678169777048716288, 2074575830148960256, 197.16, 1593136303, '1593136311', 'completed', 'pix_in'), (869785443623607552, 2388678818356235264, 422.19, 1596889520, '1596889526', 'completed', 'pix_in'), (727767235192291200, 1152700769026362752, 465.36, 1603372554, '1603372558', 'completed', 'pix_out'), (1492667345435250944, 1431539955275996672, 1515.34, 1597414796, 'None', 'failed', 'pix_in')]
# Verifying data in table: country
# Sample data in country: [('Brasil', 1811589392032273152)]