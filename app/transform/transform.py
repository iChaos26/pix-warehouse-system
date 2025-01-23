from app.database.update_dtos import TransactionDTO, TransferInDTO, TransferOutDTO, PixMovementDTO
from app.database.queries import Queries


class DataTransformer:
    """
    Handles the transformation of data from the old schema to the new schema using DTOs and Queries.
    """

    @staticmethod
    def transform_transactions(connection):
        """
        Transforms transaction-specific tables (transfer_ins, transfer_outs, pix_movements)
        into the unified 'transactions' table using DTOs and Queries.
        """
        print("Transforming old schema data into unified 'transactions' table...")

        # Use DTO for 'transactions'
        transactions_dto = TransactionDTO()

        # Transform and migrate 'transfer_ins'
        DataTransformer._migrate_table(
            connection,
            source_dto=TransferInDTO(),
            target_dto=transactions_dto,
            transformation=lambda row: (
                row["id"],
                row["account_id"],
                row["amount"],
                "transfer_in",
                row["transaction_requested_at"],
                row["transaction_completed_at"],
                row["status"],
            ),
        )
        print("Data from 'transfer_ins' migrated to 'transactions'.")

        # Transform and migrate 'transfer_outs'
        DataTransformer._migrate_table(
            connection,
            source_dto=TransferOutDTO(),
            target_dto=transactions_dto,
            transformation=lambda row: (
                row["id"],
                row["account_id"],
                row["amount"],
                "transfer_out",
                row["transaction_requested_at"],
                row["transaction_completed_at"],
                row["status"],
            ),
        )
        print("Data from 'transfer_outs' migrated to 'transactions'.")

        # Transform and migrate 'pix_movements'
        DataTransformer._migrate_table(
            connection,
            source_dto=PixMovementDTO(),
            target_dto=transactions_dto,
            transformation=lambda row: (
                row["id"],
                row["account_id"],
                row["pix_amount"],
                "pix_in" if row["in_or_out"] == "in" else "pix_out",
                row["pix_requested_at"],
                row["pix_completed_at"],
                row["status"],
            ),
        )
        print("Data from 'pix_movements' migrated to 'transactions'.")

        # Optionally, drop old tables after migration
        DataTransformer._drop_old_tables(connection)
        print("Old schema tables dropped after migration.")

    @staticmethod
    def _migrate_table(connection, source_dto, target_dto, transformation):
        """
        Handles the migration of data from a source table to a target table.

        Args:
            connection: The database connection.
            source_dto: The DTO for the source table.
            target_dto: The DTO for the target table.
            transformation: A lambda function to map source rows to target rows.
        """
        # Fetch all records from the source table
        query = Queries.fetch_all_records(source_dto)
        data = connection.execute(query).fetchall()

        # Insert transformed data into the target table
        insert_query = Queries.insert_record(target_dto)
        for row in data:
            connection.execute(insert_query, transformation(row))

    @staticmethod
    def _drop_old_tables(connection):
        """
        Drops old transaction-specific tables after data migration.
        """
        tables_to_drop = ["transfer_ins", "transfer_outs", "pix_movements"]
        for table in tables_to_drop:
            connection.execute(f"DROP TABLE IF EXISTS {table};")
