from app.database.queries import QueryBuilder
from app.database.update_dtos import (
    TransactionDTO,
    TransferInDTO,
    TransferOutDTO,
    PixMovementDTO
)

class DataTransformer:
    @staticmethod
    def transform_transactions(connection):
        """Robust transformation handling real-world data issues"""
        print("Starting schema transformation...")
        
        try:
            # 1. Create transactions table with proper schema
            DataTransformer._create_transactions_table(connection)
            
            # 2. Migrate data with explicit cleaning
            DataTransformer._migrate_legacy_data(connection)
            
            # 3. Validate essential relationships
            DataTransformer._validate_core_data(connection)
            
            # 4. Cleanup legacy tables
            DataTransformer._cleanup_legacy_tables(connection)
            
            print("\n✓ Transformation completed successfully")
            
        except Exception as e:
            print(f"\n!!! Transformation failed: {str(e)}")
            raise

    @staticmethod
    def _create_transactions_table(connection):
        """Create target table with proper data types"""
        print("Creating transactions table...")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR PRIMARY KEY,
                account_id VARCHAR,
                amount FLOAT,
                transaction_type VARCHAR,
                requested_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR
            )
        """)
        print("✓ Created transactions table")

    @staticmethod
    def _migrate_legacy_data(connection):
        """Data migration with explicit type handling"""
        print("\nMigrating legacy data:")
        
        migrations = [
            {
                "source": TransferInDTO().table_name,
                "type": "transfer_in",
                "amount": "amount",
                "timestamp": "transaction"
            },
            {
                "source": TransferOutDTO().table_name,
                "type": "transfer_out",
                "amount": "amount",
                "timestamp": "transaction"
            },
            {
                "source": PixMovementDTO().table_name,
                "type": "pix",
                "amount": "pix_amount",
                "timestamp": "pix"
            }
        ]

        for migration in migrations:
            print(f"- Processing {migration['source']}...")
            connection.execute(f"""
                INSERT INTO transactions
                SELECT
                    uuid() AS transaction_id,
                    account_id::VARCHAR,
                    {migration['amount']}::FLOAT,
                    '{migration['type']}' AS transaction_type,
                    (
                        SELECT action_timestamp 
                        FROM d_time 
                        WHERE time_id = TRY_CAST(
                            NULLIF({migration['timestamp']}_requested_at::VARCHAR, 'None') AS INTEGER
                        )
                    ),
                    (
                        SELECT action_timestamp 
                        FROM d_time 
                        WHERE time_id = TRY_CAST(
                            NULLIF({migration['timestamp']}_completed_at::VARCHAR, 'None') AS INTEGER
                        )
                    ),
                    NULLIF(status::VARCHAR, 'None')
                FROM {migration['source']}
                WHERE 
                    TRY_CAST(NULLIF({migration['timestamp']}_requested_at::VARCHAR, 'None') AS INTEGER) IS NOT NULL
                    AND TRY_CAST(NULLIF({migration['timestamp']}_completed_at::VARCHAR, 'None') AS INTEGER) IS NOT NULL
                    AND {migration['amount']}::FLOAT IS NOT NULL
            """)
            print(f"✓ Migrated {migration['source']}")

    @staticmethod
    def _validate_core_data(connection):
        """Essential data quality checks"""
        print("\nValidating core data:")
        
        # Check for invalid timestamps
        invalid_timestamps = connection.execute("""
            SELECT COUNT(*) FROM transactions
            WHERE requested_at IS NULL OR completed_at IS NULL
        """).fetchone()[0]
        print(f"Invalid timestamps: {invalid_timestamps} (allowed)")
        
        # Check account references
        orphaned_transactions = connection.execute("""
            SELECT COUNT(*) FROM transactions
            WHERE account_id NOT IN (SELECT account_id FROM accounts)
        """).fetchone()[0]
        if orphaned_transactions > 0:
            raise ValueError(f"Found {orphaned_transactions} orphaned transactions")
        
        print("✓ All core data valid")

    @staticmethod
    def _cleanup_legacy_tables(connection):
        """Safe table removal with existence checks"""
        print("\nCleaning up legacy tables:")
        for table in [TransferInDTO().table_name, 
                     TransferOutDTO().table_name,
                     PixMovementDTO().table_name]:
            if DataTransformer._table_exists(connection, table):
                connection.execute(f"DROP TABLE {table}")
                print(f"✓ Removed {table}")
            else:
                print(f"ⓘ {table} not found")

    @staticmethod
    def _table_exists(connection, table_name: str) -> bool:
        """DuckDB-compatible table existence check"""
        return connection.execute(f"""
            SELECT 1 
            FROM duckdb_tables() 
            WHERE table_name = '{table_name}'
        """).fetchone() is not None