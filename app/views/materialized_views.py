from app.database.connection import DuckDBConnection

## NEEDS TO BE DYNAMICALLY CREATED BY QUERIES AND DTOS, PER RECORD LETS STAY FROM TEST THE MVS
class MaterializedViews:
    @staticmethod
    def create_monthly_account_balances(connection):
        query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_account_balances AS
        SELECT
            accounts.account_id,
            DATE_TRUNC('month', transactions.requested_at) AS action_month,
            SUM(CASE WHEN transactions.transaction_type = 'transfer_in' THEN transactions.amount ELSE 0 END) -
            SUM(CASE WHEN transactions.transaction_type = 'transfer_out' THEN transactions.amount ELSE 0 END) AS net_balance
        FROM accounts
        LEFT JOIN transactions ON accounts.account_id = transactions.account_id
        GROUP BY accounts.account_id, DATE_TRUNC('month', transactions.requested_at);
        """
        connection.execute(query)

    @staticmethod
    def create_daily_transactions_report(connection):
        query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS daily_transactions_report AS
        SELECT
            DATE(transactions.requested_at) AS transaction_date,
            SUM(CASE WHEN transactions.transaction_type = 'transfer_in' THEN transactions.amount ELSE 0 END) AS total_transfer_in,
            SUM(CASE WHEN transactions.transaction_type = 'transfer_out' THEN transactions.amount ELSE 0 END) AS total_transfer_out,
            SUM(CASE WHEN transactions.transaction_type = 'pix_in' THEN transactions.amount ELSE 0 END) AS total_pix_in,
            SUM(CASE WHEN transactions.transaction_type = 'pix_out' THEN transactions.amount ELSE 0 END) AS total_pix_out
        FROM transactions
        GROUP BY DATE(transactions.requested_at);
        """
        connection.execute(query)


# if __name__ == "__main__":
#     db_connection = DuckDBConnection()
#     connection = db_connection.connect()

#     try:
#         print("Creating materialized views...")
#         MaterializedViews.create_monthly_account_balances(connection)
#         MaterializedViews.create_daily_transactions_report(connection)
#         print("Materialized views created successfully!")
#     finally:
#         db_connection.close()
