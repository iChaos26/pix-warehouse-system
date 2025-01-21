from app.mock import MockDataGenerator
def test_mock_data_insertion(db_connection):
    # Generate mock data
    generator = MockDataGenerator()
    transactions = generator.generate_transactions(50)

    # Insert mock data into the unified transactions table
    db_connection.execute(
        """
        CREATE TABLE transactions (
            transaction_id UUID,
            account_id UUID,
            amount DECIMAL(18, 2),
            transaction_type VARCHAR,
            requested_at TIMESTAMP,
            completed_at TIMESTAMP,
            status VARCHAR
        );
        """
    )
    for row in transactions:
        db_connection.execute(
            "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?);",
            (
                row["transaction_id"],
                row["account_id"],
                row["amount"],
                row["transaction_type"],
                row["requested_at"],
                row["completed_at"],
                row["status"],
            ),
        )

    # Validate insertion
    result = db_connection.execute("SELECT COUNT(*) FROM transactions;").fetchone()[0]
    assert result == len(transactions)
