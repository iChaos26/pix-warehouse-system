from app.database.queries import QueryBuilder
from app.database.update_dtos import TransactionDTO, AccountDTO, CustomerDTO

class MaterializedViews:
    @staticmethod
    def create_monthly_account_balances(connection):
        """Calculates rolling monthly balances with carryover"""
        print("Creating monthly account balances view...")
        
        connection.execute("""
            CREATE OR REPLACE VIEW monthly_account_balances AS
            WITH monthly_net AS (
                SELECT
                    account_id,
                    DATE_TRUNC('month', requested_at) AS month,
                    SUM(CASE
                        WHEN transaction_type IN ('transfer_in', 'pix_in') THEN amount
                        ELSE -amount
                    END) AS net_change
                FROM transactions
                WHERE requested_at BETWEEN '2020-01-01' AND '2020-12-31'
                GROUP BY account_id, DATE_TRUNC('month', requested_at)
            )
            SELECT
                account_id,
                month,
                SUM(net_change) OVER (
                    PARTITION BY account_id 
                    ORDER BY month 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS account_balance
            FROM monthly_net
            ORDER BY account_id, month;
        """)
        
        MaterializedViews._validate_view_creation(connection, "monthly_account_balances")
        print("✓ Created monthly account balances view")

    @staticmethod
    def create_daily_transactions_report(connection):
        """Fixed date handling for DuckDB"""
        print("Creating daily transactions report view...")
        
        connection.execute("""
            CREATE OR REPLACE VIEW daily_transactions_report AS
            SELECT
                CAST(requested_at AS DATE) AS transaction_date,
                COALESCE(SUM(CASE WHEN transaction_type = 'transfer_in' THEN amount END), 0) AS total_transfer_in,
                COALESCE(SUM(CASE WHEN transaction_type = 'transfer_out' THEN amount END), 0) AS total_transfer_out,
                COALESCE(SUM(CASE WHEN transaction_type = 'pix_in' THEN amount END), 0) AS total_pix_in,
                COALESCE(SUM(CASE WHEN transaction_type = 'pix_out' THEN amount END), 0) AS total_pix_out
            FROM transactions
            GROUP BY CAST(requested_at AS DATE)
        """)
        
        MaterializedViews._validate_view_creation(connection, "daily_transactions_report")
        print("✓ Created daily transactions report view")

    @staticmethod
    def _validate_view_creation(connection, view_name):
        """Enhanced view validation with error details"""
        try:
            # Check view existence
            exists = connection.execute(f"""
                SELECT COUNT(*) 
                FROM duckdb_views() 
                WHERE view_name = '{view_name}'
            """).fetchone()[0] > 0
            
            # Check basic queryability
            test_query = connection.execute(f"""
                SELECT * FROM {view_name} LIMIT 1
            """)
            
            if not exists or test_query.fetchone() is None:
                raise ValueError(f"View {view_name} creation failed validation")
                
        except Exception as e:
            raise RuntimeError(f"View validation failed for {view_name}: {str(e)}")