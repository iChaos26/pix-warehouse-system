class DatabaseSchema:
    @staticmethod
    def create_schema(connection):
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
            """,
            """
            CREATE TABLE transfer_ins (
                id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                amount FLOAT,
                transaction_requested_at INT,
                transaction_completed_at INT,
                status VARCHAR(128)
            );
            """,
            """
            CREATE TABLE transfer_outs (
                id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                amount FLOAT,
                transaction_requested_at INT,
                transaction_completed_at INT,
                status VARCHAR(128)
            );
            """,
            """
            CREATE TABLE pix_movements (
                id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                in_or_out VARCHAR(128),
                pix_amount FLOAT,
                pix_requested_at INT,
                pix_completed_at INT,
                status VARCHAR(128)
            );
            """
        ]

        for query in queries:
            connection.execute(query)
