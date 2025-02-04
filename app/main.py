import os
import numpy
from prettytable import PrettyTable
from app.database.connection import DuckDBConnection
from app.transform.transform import DataTransformer
from app.database.queries import QueryBuilder
from app.database.update_dtos import PixMovementDTO, CountryDTO, CustomerDTO, AccountDTO, TransferInDTO, TransferOutDTO, TransactionDTO
from app.views.materialized_views import MaterializedViews
from app.views.transactions_views import TransactionsViews

#!TO DO: REFACTOR DATAMANAGER AND DATA TESTS
#- DATA MANAGER AS A MODULE OR API
#- UNIT TESTS AND VIEW TESTS USING DUCKDB 
#- LOGGER
#- VIEWS DTOS, REFACTOR VIEWS DIRECT QUERY EXECUTIONS 
#- UPDATE DOCKER COMPOSE TO A PERSISTENT DATABASE WITH DB-API INTERFACE IN fsspec(FS) OR POSTGRES SQL: FILE FORMAT PARQUET
#- DATA LOADER AS A MODULE
#- AWS AND AZURE PLUGINS: S3 AND BLOB OR REDSHIFT + SECRETS MANAGER
#- FUTURE: DEPLOY ON K8S WITH GITHUB ACTIONS OR BB PIPELINES(AZURE DEVOPS)
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
        
        for root, _, files in os.walk(self.csv_folder):
            for file in files:
                if file.endswith(".csv"):
                    table_name = os.path.basename(root)  # Use folder name as the table name
                    csv_file_path = os.path.join(root, file)
                    print(f"Loading data from {csv_file_path} into {table_name}...")

                    # Create table and load data
                    self.connection.sql(
                        f"CREATE TABLE {table_name} AS "
                        f"SELECT * FROM read_csv('{csv_file_path}', AUTO_DETECT=TRUE)"
                    )
                    print(f"Data from {csv_file_path} loaded successfully into {table_name}.")

    def perform_transformation(self):
        """Executes the full transformation workflow"""
        print("Starting schema transformation...")
        DataTransformer.transform_transactions(self.connection)
        print("Schema transformation completed successfully!")

    def create_materialized_views(self):
        """Creates all reporting views"""
        print("Building materialized views...")
        MaterializedViews.create_monthly_account_balances(self.connection)
        MaterializedViews.create_daily_transactions_report(self.connection)
        TransactionsViews.create_customer_financial_overview(self.connection)
        TransactionsViews.create_top_performing_accounts(self.connection)
        print("Materialized views created successfully.")
    
    def analyze_accounts(self, account_ids):
        """Analyze specific accounts using materialized views"""
        print("\n=== ACCOUNT ANALYSIS ===")
        
        if not account_ids:
            print("No account IDs provided")
            return

        # 1. Get monthly balances
        monthly_balances = self.connection.execute(f"""
            SELECT 
                account_id,
                strftime(month, '%Y-%m') AS month,
                account_balance
            FROM monthly_account_balances
            WHERE account_id IN {tuple(account_ids)}
            ORDER BY account_id, month
        """).fetchall()

        # 2. Get performance rankings
        performance_data = self.connection.execute(f"""
            SELECT 
                tpa.account_id,
                tpa.performance_rank,
                c.first_name || ' ' || c.last_name AS customer_name,
                tpa.total_incoming
            FROM top_performing_accounts tpa
            JOIN accounts a ON tpa.account_id = a.account_id
            JOIN customers c ON a.customer_id = c.customer_id
            WHERE tpa.account_id IN {tuple(account_ids)}
        """).fetchall()

        # Display results
        print("\nMonthly Account Balances:")
        balance_table = PrettyTable()
        balance_table.field_names = ["Account ID", "Month", "Balance"]
        for row in monthly_balances:
            balance_table.add_row([row[0], row[1], f"R${row[2]:,.2f}"])
        print(balance_table)

        print("\nAccount Performance:")
        perf_table = PrettyTable()
        perf_table.field_names = ["Account ID", "Rank", "Customer", "Total Incoming"]
        for row in performance_data:
            perf_table.add_row([
                row[0],
                row[1],
                row[2],
                f"R${row[3]:,.2f}"
            ])
        print(perf_table)

    def analyze_transactions(self, customer_ids):
        """Analyze customer transactions using materialized views"""
        print("\n=== TRANSACTION ANALYSIS ===")
        
        if not customer_ids:
            print("No customer IDs provided")
            return

        # 1. Get financial overview
        financial_overview = self.connection.execute(f"""
            SELECT 
                customer_id,
                total_transfer_in,
                total_transfer_out,
                total_pix_in,
                total_pix_out
            FROM customer_financial_overview
            WHERE customer_id IN {tuple(customer_ids)}
        """).fetchall()
        # 2. Fixed temporal patterns query
        temporal_patterns = self.connection.execute(f"""
            SELECT
                strftime(transaction_date, '%Y-%m') AS month,
                SUM(total_transfer_in + total_transfer_out + total_pix_in + total_pix_out) AS total_volume,
                COUNT(*) AS transaction_days
            FROM daily_transactions_report
            WHERE EXISTS (
                SELECT 1 FROM transactions t
                JOIN accounts a ON t.account_id = a.account_id
                WHERE a.customer_id IN {tuple(customer_ids)}
                AND CAST(t.requested_at AS DATE) = transaction_date
            )
            GROUP BY month
            ORDER BY month
        """).fetchall()

        # Display results
        print("\nFinancial Overview:")
        finance_table = PrettyTable()
        finance_table.field_names = [
            "Customer ID", "Transfers In", "Transfers Out", "PIX In", "PIX Out"
        ]
        for row in financial_overview:
            finance_table.add_row([
                row[0],
                f"R${row[1]:,.2f}",
                f"R${row[2]:,.2f}",
                f"R${row[3]:,.2f}",
                f"R${row[4]:,.2f}"
            ])
        print(finance_table)

        print("\nMonthly Activity Patterns:")
        temporal_table = PrettyTable()
        temporal_table.field_names = ["Month", "Total Volume", "Active Days"]
        for row in temporal_patterns:
            temporal_table.add_row([
                row[0],
                f"R${row[1]:,.2f}",
                row[2]
            ])
        print(temporal_table)

    def validate_data_ingestion(self):
        """Robust validation with error containment"""
        print("\nValidating data ingestion:")
        for table, dto in [
            ("pix_movements", PixMovementDTO()),
            ("country", CountryDTO()),
            ("accounts", AccountDTO()),
            ("transfer_ins", TransferInDTO()),
            ("transfer_out", TransferOutDTO()),
        ]:
            try:
                # Simple select with limit
                query = QueryBuilder.select(dto, limit=5)
                result = self.connection.execute(query).fetchall()
                print(f"  {table}: Found {len(result)} records")
                if result:
                    print(f"    Sample: {result[0]}")
            except Exception as e:
                print(f"  Validation failed for {table}: {type(e).__name__} - {str(e)}")

    def test_query_builder(self):
        """Comprehensive test with error diagnostics"""
        print("\nTesting QueryBuilder functionality:")
        
        test_cases = [
            {
                "name": "Country Filter",
                "dto": CountryDTO(),
                "filters": {"country": "'Brasil'"},
                "expected": 1
            },
            {
                "name": "Customer Aggregation",
                "dto": CustomerDTO(),
                "aggregates": {
                    "total": "COUNT(*)",
                    "GROUP BY": "country_name"
                },
                "expected": ">=1"
            },
            {
                "name": "Transaction Aggregation",
                "dto": TransactionDTO(),
                "aggregates": {
                    "total_amount": "SUM(amount)"
                },
                "expected": ">=1"
            },
        
        ]
        
        for case in test_cases:
            try:
                print(f"\nTest: {case['name']}")
                query = QueryBuilder.select(
                    case["dto"],
                    filters=case.get("filters"),
                    aggregates=case.get("aggregates")
                )
                print(f"Generated query:\n{query}")
                
                result = self.connection.execute(query).fetchall()
                print(f"Results: {result}")
                print(f"Results: {len(result)} rows")
                
                if isinstance(case["expected"], int):
                    assert len(result) == case["expected"], "Unexpected result count"
                elif case["expected"].startswith(">="):
                    min_count = int(case["expected"][2:])
                    assert len(result) >= min_count, "Insufficient results"
                    
            except Exception as e:
                print(f"Test failed: {type(e).__name__} - {str(e)}")
                raise #Preserve stack trace

    def validate_dto_schema(self):
        """Ensure DTOs match table schemas"""
        print("\nValidating DTO schemas:")
        for dto in [CustomerDTO(), CountryDTO(), AccountDTO()]:
            result = self.connection.sql(f"DESCRIBE {dto.table_name}").fetchall()
            actual_columns = [row[0] for row in result]
            
            print(f"\n{dto.table_name.upper()} validation:")
            print(f"DTO expects: {dto.columns}")
            print(f"Table has:   {actual_columns}")
        
        assert set(dto.columns) == set(actual_columns), "Schema mismatch!"
    print("\nAll DTO schemas validated successfully!")


def main():
    db = DuckDBConnection()

    try:
        manager = DataManager(db.connect())
        
        print("\n=== DATA INGESTION ===")
        manager.load_csv_data()
        print("\n=== VALIDATING DATA INGESTION ===")
        #WORKING AND VALIDATED
        manager.validate_data_ingestion()
        #WORKING AND VALIDATED
        print("\n=== SCHEMA VALIDATION ===")
        manager.validate_dto_schema()

        print("\n=== SCHEMA TRANSFORMATION ===")
        manager.perform_transformation()

        print("\n=== QUERY BUILDER TESTS ===")
        manager.test_query_builder()
        

        print("\n=== VIEW GENERATION ===")
        manager.create_materialized_views()

        # Test Account IDs
        account_ids = [
            '1095295572704434176', '3331826451769351680',
            '231771070487223648', '909292279053935488',
            '1372963006028127232', '1139129464680807424',
            '2173517564649275392', '476448784420957056',
            '1493429928567988480', '817858258609867392',
            '507987780945996736', '336693543816500544',
            '414591092625732800', '1669481937421247488',
            '712444109815960448', '2272171310327071744',
            '2669626301710158848', '1158353061834253312',
            '242604038203577184', '1389881518493714688',
            '1396423087886678016', '500443057508466112',
            '3011375634010832896', '1987395201418850560',
            '2922610483805172224', '554916756082622784',
            '2352407409595471360', '1749186158767626496',
            '831465696157769088', '2259747515549796096'
        ]

        # Test Customer IDs
        customer_ids = [
            '3331826451769351680', '909292279053935488',
            '1139129464680807424', '476448784420957056',
            '817858258609867392', '336693543816500544',
            '1669481937421247488', '2272171310327071744',
            '1158353061834253312', '1389881518493714688',
            '500443057508466112', '1987395201418850560',
            '554916756082622784', '1749186158767626496',
            '2259747515549796096'
        ]

        print("\n=== ACCOUNT ANALYSIS ===")
        manager.analyze_accounts(account_ids)

        print("\n=== TRANSACTION ANALYSIS ===")
        manager.analyze_transactions(customer_ids)

        print("\n=== TABLE DISPLAY ===")
        #print(db.connect().sql("SHOW ALL TABLES").df())
        schema_arr = db.connect().sql("SHOW ALL TABLES").show()
        print(schema_arr)
        print("\n=== PIPELINE COMPLETE ===")

    except Exception as e:
        print(f"\n!!! PIPELINE FAILED: {str(e)}")
    finally:
        db.close()


if __name__ == "__main__":
    main()

#OUTPUT VALIDATION
# Validating data ingestion:
# Data from data/customers/part-00000-tid-2581180368179082894-038c4dab-1615-444a-994f-5a2107c4d9a8-11135734-1-c000.csv loaded successfully into customers.
# Loading data from data/d_time/part-00000-tid-2902183611695287462-3111e3e9-023d-4533-a7e5-d0b87fb1a808-10662337-1-c000.csv into d_time...
# Data from data/d_time/part-00000-tid-2902183611695287462-3111e3e9-023d-4533-a7e5-d0b87fb1a808-10662337-1-c000.csv loaded successfully into d_time.
#   pix_movements: Found 2 records
#     Sample: (3101512580483427328, 127700825787235152, 'pix_in', 1.56, 1587921566, 'None', 'failed')
#   country: Found 1 records
#     Sample: (1811589392032273152, 'Brasil')
#   accounts: Found 2 records
#     Sample: (127700825787235152, 3018741143504866816, datetime.datetime(2019, 4, 19, 1, 34, 25), 'active', 8366, 3, 41002)
# === SCHEMA VALIDATION ===
# Validating DTO schemas:

# CUSTOMERS validation:
# DTO expects: ['customer_id', 'first_name', 'last_name', 'customer_city', 'cpf', 'country_name']
# Table has:   ['customer_id', 'first_name', 'last_name', 'customer_city', 'cpf', 'country_name']

# COUNTRY validation:
# DTO expects: ['country_id', 'country']
# Table has:   ['country', 'country_id']

# ACCOUNTS validation:
# DTO expects: ['account_id', 'customer_id', 'created_at', 'status', 'account_branch', 'account_check_digit', 'account_number']
# Table has:   ['account_id', 'customer_id', 'created_at', 'status', 'account_branch', 'account_check_digit', 'account_number']


# === SCHEMA TRANSFORMATION ===
# Starting schema transformation...
# Starting schema transformation...
# Creating transactions table...
# ✓ Created transactions table

# Migrating legacy data:
# - Processing transfer_ins...
# ✓ Migrated transfer_ins
# - Processing transfer_outs...
# ✓ Migrated transfer_outs
# - Processing pix_movements...
# ✓ Migrated pix_movements

# Validating core data:
# Invalid timestamps: 0 (allowed)
# ✓ All core data valid

# Cleaning up legacy tables:
# ✓ Removed transfer_ins
# ✓ Removed transfer_outs
# ✓ Removed pix_movements

# ✓ Transformation completed successfully
# Schema transformation completed successfully!

# === QUERY BUILDER TESTS ===

# Testing QueryBuilder functionality:

# Test: Country Filter
# Generated query:
# SELECT country_id, country FROM country WHERE country = 'Brasil';
# Results: [(1811589392032273152, 'Brasil')]
# Results: 1 rows

# Test: Customer Aggregation
# Generated query:
# SELECT COUNT(*) AS total FROM customers GROUP BY country_name;
# Results: [(4000,)]
# Results: 1 rows

# Test: Transaction Aggregation
# Generated query:
# SELECT SUM(amount) AS total_amount FROM transactions;
# Results: [(476210790.032836,)]
# Results: 1 rows

# Monthly Account Balances:
# +---------------------+---------+-------------+
# |      Account ID     |  Month  |   Balance   |
# +---------------------+---------+-------------+
# | 1095295572704434176 | 2020-01 |  R$1,507.66 |
# | 1095295572704434176 | 2020-03 |  R$1,867.62 |
# | 1095295572704434176 | 2020-12 |  R$1,237.43 |
# | 1372963006028127232 | 2020-04 |  R$1,553.89 |
# | 1372963006028127232 | 2020-05 |   R$573.95  |
# | 1396423087886678016 | 2020-03 |  R$-778.64  |
# | 1396423087886678016 | 2020-04 | R$-1,698.05 |
# | 1396423087886678016 | 2020-07 | R$-3,514.57 |
# | 1493429928567988480 | 2020-04 |  R$1,795.00 |
# | 1493429928567988480 | 2020-08 |  R$1,373.81 |
# | 1493429928567988480 | 2020-12 |  R$1,086.54 |
# | 2173517564649275392 | 2020-04 |  R$-767.67  |
# | 2173517564649275392 | 2020-06 | R$-1,434.18 |
# | 2173517564649275392 | 2020-08 | R$-1,538.05 |
# |  231771070487223648 | 2020-07 | R$-2,752.98 |
# |  231771070487223648 | 2020-09 | R$-1,368.83 |
# | 2352407409595471360 | 2020-03 |   R$957.00  |
# | 2352407409595471360 | 2020-06 |  R$-770.59  |
# |  242604038203577184 | 2020-09 |   R$195.14  |
# |  242604038203577184 | 2020-10 | R$-1,477.75 |
# | 2669626301710158848 | 2020-05 |  R$-663.03  |
# | 2669626301710158848 | 2020-06 |  R$-960.54  |
# | 2669626301710158848 | 2020-08 |  R$-344.86  |
# | 2922610483805172224 | 2020-01 |  R$1,776.69 |
# | 2922610483805172224 | 2020-02 |  R$4,500.25 |
# | 3011375634010832896 | 2020-02 | R$-1,147.33 |
# | 3011375634010832896 | 2020-11 | R$-1,448.53 |
# |  414591092625732800 | 2020-02 |  R$1,661.28 |
# |  414591092625732800 | 2020-09 |   R$259.70  |
# |  414591092625732800 | 2020-11 |  R$-379.65  |
# |  507987780945996736 | 2020-04 |   R$971.68  |
# |  507987780945996736 | 2020-10 |  R$-339.06  |
# |  712444109815960448 | 2020-02 |   R$790.28  |
# |  712444109815960448 | 2020-05 |  R$1,189.08 |
# |  712444109815960448 | 2020-11 |  R$2,495.55 |
# |  831465696157769088 | 2020-02 |  R$-407.94  |
# |  831465696157769088 | 2020-03 |   R$585.52  |
# |  831465696157769088 | 2020-04 |   R$-45.63  |
# +---------------------+---------+-------------+

# Account Performance:
# +---------------------+------+-------------------+----------------+
# |      Account ID     | Rank |      Customer     | Total Incoming |
# +---------------------+------+-------------------+----------------+
# | 2922610483805172224 | 3774 |    John Phelps    |   R$4,500.25   |
# |  712444109815960448 | 3862 |     Mary Olson    |   R$2,495.55   |
# |  414591092625732800 | 3902 |   Sharron Fields  |   R$1,661.28   |
# |  231771070487223648 | 3916 |     Candy Vail    |   R$1,384.15   |
# |  831465696157769088 | 3934 |   Archie Elliot   |    R$993.46    |
# |  507987780945996736 | 3935 |   Crystal Davis   |    R$971.68    |
# | 2352407409595471360 | 3936 | Priscilla Redding |    R$957.00    |
# | 2669626301710158848 | 3948 |   Steven Hollis   |    R$615.68    |
# |  242604038203577184 | 3952 | Frederick Laborde |    R$492.33    |
# | 1396423087886678016 | 3962 |   Ted Spachtholz  |     R$0.00     |
# | 3011375634010832896 | 3962 |    Jesse Krapp    |     R$0.00     |
# | 1372963006028127232 | 3863 |   Joseph Bennett  |   R$2,475.62   |
# | 1493429928567988480 | 3898 |   Marlene Suchan  |   R$1,795.00   |
# | 2173517564649275392 | 3962 |   Frank Thompson  |     R$0.00     |
# | 1095295572704434176 | 3890 |   Carol Maultsby  |   R$1,867.62   |
# +---------------------+------+-------------------+----------------+
# === TRANSACTION ANALYSIS ===

# === TRANSACTION ANALYSIS ===

# Financial Overview:
# +---------------------+--------------+---------------+--------+---------+
# |     Customer ID     | Transfers In | Transfers Out | PIX In | PIX Out |
# +---------------------+--------------+---------------+--------+---------+
# |  476448784420957056 |    R$0.00    |    R$103.87   | R$0.00 |  R$0.00 |
# |  336693543816500544 |   R$971.68   |    R$489.15   | R$0.00 |  R$0.00 |
# | 1669481937421247488 |  R$1,661.28  |    R$639.35   | R$0.00 |  R$0.00 |
# | 1987395201418850560 |    R$0.00    |    R$301.20   | R$0.00 |  R$0.00 |
# | 3331826451769351680 |  R$1,867.62  |     R$0.00    | R$0.00 |  R$0.00 |
# |  909292279053935488 |  R$1,384.15  |     R$0.00    | R$0.00 |  R$0.00 |
# | 1139129464680807424 |  R$2,475.62  |     R$0.00    | R$0.00 |  R$0.00 |
# |  817858258609867392 |  R$1,795.00  |     R$0.00    | R$0.00 |  R$0.00 |
# | 2272171310327071744 |  R$2,495.55  |     R$0.00    | R$0.00 |  R$0.00 |
# | 1158353061834253312 |   R$615.68   |     R$0.00    | R$0.00 |  R$0.00 |
# | 1389881518493714688 |   R$492.33   |     R$0.00    | R$0.00 |  R$0.00 |
# |  554916756082622784 |  R$4,500.25  |     R$0.00    | R$0.00 |  R$0.00 |
# | 1749186158767626496 |   R$957.00   |     R$0.00    | R$0.00 |  R$0.00 |
# | 2259747515549796096 |   R$993.46   |     R$0.00    | R$0.00 |  R$0.00 |
# |  500443057508466112 |    R$0.00    |     R$0.00    | R$0.00 |  R$0.00 |
# +---------------------+--------------+---------------+--------+---------+

# Monthly Activity Patterns:
# +---------+----------------+-------------+
# |  Month  |  Total Volume  | Active Days |
# +---------+----------------+-------------+
# | 2020-01 | R$1,312,655.45 |      2      |
# | 2020-02 | R$4,443,655.60 |      7      |
# | 2020-03 | R$2,640,361.31 |      4      |
# | 2020-04 | R$4,002,891.03 |      6      |
# | 2020-05 | R$2,512,478.83 |      4      |
# | 2020-06 | R$1,960,416.79 |      3      |
# | 2020-07 | R$1,891,186.03 |      3      |
# | 2020-08 | R$1,988,396.67 |      3      |
# | 2020-09 | R$2,644,157.33 |      4      |
# | 2020-10 | R$1,376,477.55 |      2      |
# | 2020-11 | R$1,941,739.07 |      3      |
# | 2020-12 | R$1,263,625.36 |      2      |
# +---------+----------------+-------------+