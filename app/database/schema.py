class DatabaseSchema:
    """
    Handles the creation of database schemas for old and new systems.
    """

    @staticmethod
    def create_old_schema(connection):
        """
        Creates the old schema, which includes dimension tables and transaction-specific tables.
        """
        queries = [
            """
            CREATE TABLE d_week (
                week_id INT PRIMARY KEY,
                action_week INT
            );
            """,
            """
            CREATE TABLE d_weekday (
                weekday_id INT PRIMARY KEY,
                action_weekday VARCHAR(128)
            );
            """,
            """
            CREATE TABLE d_month (
                month_id INT PRIMARY KEY,
                action_month INT
            );
            """,
            """
            CREATE TABLE d_year (
                year_id INT PRIMARY KEY,
                action_year INT
            );
            """,
            """
            CREATE TABLE d_time (
                time_id INT PRIMARY KEY,
                action_timestamp TIMESTAMP,
                week_id INT REFERENCES d_week(week_id),
                month_id INT REFERENCES d_month(month_id),
                year_id INT REFERENCES d_year(year_id),
                weekday_id INT REFERENCES d_weekday(weekday_id)
            );
            """,
            """
            CREATE TABLE transfer_ins (
                id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                amount FLOAT,
                transaction_requested_at INT REFERENCES d_time(time_id),
                transaction_completed_at INT REFERENCES d_time(time_id),
                status VARCHAR(128)
            );
            """,
            """
            CREATE TABLE transfer_outs (
                id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                amount FLOAT,
                transaction_requested_at INT REFERENCES d_time(time_id),
                transaction_completed_at INT REFERENCES d_time(time_id),
                status VARCHAR(128)
            );
            """,
            """
            CREATE TABLE pix_movements (
                id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                in_or_out VARCHAR(128),
                pix_amount FLOAT,
                pix_requested_at INT REFERENCES d_time(time_id),
                pix_completed_at INT REFERENCES d_time(time_id),
                status VARCHAR(128)
            );
            """
        ]

        for query in queries:
            connection.execute(query)

    @staticmethod
    def create_new_schema(connection):
        """
        Creates the new schema, which consolidates transaction tables into a unified 'transactions' table.
        """
        queries = [
            """
            CREATE TABLE transactions (
                transaction_id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                amount FLOAT,
                transaction_type VARCHAR(128),
                requested_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR(128)
            );
            """
        ]

        for query in queries:
            connection.execute(query)

    @staticmethod
    def create_core_schema(connection):
        """
        Creates core tables that are shared between old and new schemas.
        """
        queries = [
            """
            CREATE TABLE country (
                country_id UUID PRIMARY KEY,
                country VARCHAR(128)
            );
            """,
            """
            CREATE TABLE state (
                state_id UUID PRIMARY KEY,
                state VARCHAR(128),
                country_id UUID REFERENCES country(country_id)
            );
            """,
            """
            CREATE TABLE city (
                city_id INT PRIMARY KEY,
                city VARCHAR(256),
                state_id UUID REFERENCES state(state_id)
            );
            """,
            """
            CREATE TABLE customers (
                customer_id UUID PRIMARY KEY,
                first_name VARCHAR(128),
                last_name VARCHAR(128),
                customer_city INT REFERENCES city(city_id),
                country_name VARCHAR(128),
                cpf INT
            );
            """,
            """
            CREATE TABLE accounts (
                account_id UUID PRIMARY KEY,
                customer_id UUID REFERENCES customers(customer_id),
                created_at TIMESTAMP,
                status VARCHAR(128),
                account_branch VARCHAR(128),
                account_check_digit VARCHAR(128),
                account_number VARCHAR(128)
            );
            """
        ]

        for query in queries:
            connection.execute(query)

    @staticmethod
    def create_schema(connection):
        """
        Creates the entire schema, combining old, new, and core tables.
        """
        DatabaseSchema.create_core_schema(connection)
        DatabaseSchema.create_old_schema(connection)
        DatabaseSchema.create_new_schema(connection)
