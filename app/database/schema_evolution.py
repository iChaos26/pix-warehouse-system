from database.connection import DuckDBConnection


class SchemaEvolution:
    @staticmethod
    def transform_to_new_schema(connection):
        """
        Transforms data from the old schema into the new schema.
        Consolidates transaction-specific tables into the unified 'transactions' table.
        """
        print("Transforming data from old schema to new schema...")

        # Consolidate transfer_ins, transfer_outs, and pix_movements into 'transactions'
        connection.execute(
            """
            INSERT INTO transactions
            SELECT
                id AS transaction_id,
                account_id,
                amount,
                'transfer_in' AS transaction_type,
                transaction_requested_at AS requested_at,
                transaction_completed_at AS completed_at,
                status
            FROM transfer_ins;
            """
        )
        print("Data from 'transfer_ins' migrated to 'transactions'.")

        connection.execute(
            """
            INSERT INTO transactions
            SELECT
                id AS transaction_id,
                account_id,
                amount,
                'transfer_out' AS transaction_type,
                transaction_requested_at AS requested_at,
                transaction_completed_at AS completed_at,
                status
            FROM transfer_outs;
            """
        )
        print("Data from 'transfer_outs' migrated to 'transactions'.")

        connection.execute(
            """
            INSERT INTO transactions
            SELECT
                id AS transaction_id,
                account_id,
                pix_amount AS amount,
                CASE
                    WHEN in_or_out = 'in' THEN 'pix_in'
                    WHEN in_or_out = 'out' THEN 'pix_out'
                END AS transaction_type,
                pix_requested_at AS requested_at,
                pix_completed_at AS completed_at,
                status
            FROM pix_movements;
            """
        )
        print("Data from 'pix_movements' migrated to 'transactions'.")

        # Optionally, drop old tables after migration
        connection.execute("DROP TABLE IF EXISTS transfer_ins;")
        connection.execute("DROP TABLE IF EXISTS transfer_outs;")
        connection.execute("DROP TABLE IF EXISTS pix_movements;")
        print("Old schema tables dropped after migration.")
