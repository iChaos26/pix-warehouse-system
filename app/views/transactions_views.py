from app.database.connection import DuckDBConnection

## NEEDS TO BE DYNAMICALLY CREATED BY QUERIES AND DTOS, PER RECORD LETS STAY FROM TEST THE MVS
class TransactionsViews:
    @staticmethod
    def create_customer_financial_overview(connection):
        query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS customer_financial_overview AS
        SELECT
            customers.customer_id,
            customers.first_name,
            customers.last_name,
            SUM(CASE WHEN transactions.transaction_type = 'transfer_in' THEN transactions.amount ELSE 0 END) AS total_transfer_in,
            SUM(CASE WHEN transactions.transaction_type = 'transfer_out' THEN transactions.amount ELSE 0 END) AS total_transfer_out,
            SUM(CASE WHEN transactions.transaction_type = 'pix_in' THEN transactions.amount ELSE 0 END) AS total_pix_in,
            SUM(CASE WHEN transactions.transaction_type = 'pix_out' THEN transactions.amount ELSE 0 END) AS total_pix_out
        FROM customers
        LEFT JOIN accounts ON customers.customer_id = accounts.customer_id
        LEFT JOIN transactions ON accounts.account_id = transactions.account_id
        GROUP BY customers.customer_id;
        """
        connection.execute(query)

    @staticmethod
    def create_top_performing_accounts(connection):
        query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS top_performing_accounts AS
        SELECT
            accounts.account_id,
            SUM(CASE WHEN transactions.transaction_type = 'transfer_in' THEN transactions.amount ELSE 0 END) AS total_transfer_in,
            SUM(CASE WHEN transactions.transaction_type = 'pix_in' THEN transactions.amount ELSE 0 END) AS total_pix_in,
            SUM(CASE WHEN transactions.transaction_type IN ('transfer_in', 'pix_in') THEN transactions.amount ELSE 0 END) AS total_incoming,
            RANK() OVER (ORDER BY SUM(CASE WHEN transactions.transaction_type IN ('transfer_in', 'pix_in') THEN transactions.amount ELSE 0 END) DESC) AS rank
        FROM accounts
        LEFT JOIN transactions ON accounts.account_id = transactions.account_id
        GROUP BY accounts.account_id;
        """
        connection.execute(query)


# if __name__ == "__main__":
#     db_connection = DuckDBConnection()
#     connection = db_connection.connect()

#     try:
#         print("Creating transaction views...")
#         TransactionsViews.create_customer_financial_overview(connection)
#         TransactionsViews.create_top_performing_accounts(connection)
#         print("Transaction views created successfully!")
#     finally:
#         db_connection.close()
