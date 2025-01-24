from app.database.queries import QueryBuilder
from app.database.update_dtos import TransactionDTO, AccountDTO, CustomerDTO
class TransactionsViews:
    @staticmethod
    def create_customer_financial_overview(connection):
        """Customer overview with proper join strategy"""
        print("Creating customer financial overview view...")
        
        connection.execute("""
            CREATE OR REPLACE VIEW customer_financial_overview AS
            SELECT
                c.customer_id,
                c.first_name,
                c.last_name,
                COALESCE(SUM(CASE WHEN t.transaction_type = 'transfer_in' THEN t.amount END), 0) AS total_transfer_in,
                COALESCE(SUM(CASE WHEN t.transaction_type = 'transfer_out' THEN t.amount END), 0) AS total_transfer_out,
                COALESCE(SUM(CASE WHEN t.transaction_type = 'pix_in' THEN t.amount END), 0) AS total_pix_in,
                COALESCE(SUM(CASE WHEN t.transaction_type = 'pix_out' THEN t.amount END), 0) AS total_pix_out
            FROM customers c
            LEFT JOIN accounts a ON c.customer_id = a.customer_id
            LEFT JOIN transactions t ON a.account_id = t.account_id
            GROUP BY c.customer_id, c.first_name, c.last_name
        """)
        
        TransactionsViews._validate_view_data(connection, "customer_financial_overview")
        print("✓ Created customer financial overview view")

    @staticmethod
    def create_top_performing_accounts(connection):
        """Ranking view with window function fix"""
        print("Creating top performing accounts view...")
        
        connection.execute("""
            CREATE OR REPLACE VIEW top_performing_accounts AS
            WITH account_performance AS (
                SELECT
                    a.account_id,
                    COALESCE(SUM(CASE WHEN t.transaction_type IN ('transfer_in', 'pix_in') THEN t.amount END), 0) AS total_incoming
                FROM accounts a
                LEFT JOIN transactions t ON a.account_id = t.account_id
                GROUP BY a.account_id
            )
            SELECT
                account_id,
                total_incoming,
                RANK() OVER (ORDER BY total_incoming DESC) AS performance_rank
            FROM account_performance
        """)
        
        TransactionsViews._validate_view_data(connection, "top_performing_accounts")
        print("✓ Created top performing accounts view")

    @staticmethod
    def _validate_view_data(connection, view_name):
        """Basic data sanity check"""
        count = connection.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        if count == 0:
            print(f"⚠️ Warning: {view_name} contains no data")