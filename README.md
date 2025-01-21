# Nubank Test Project

This project is designed to simulate and test functionalities of a financial system using Python, Docker, and Poetry for dependency management. It includes modular components for database management, account balances, transaction handling, and testing. Below is a comprehensive guide to setting up and using the project.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Setup and Installation](#setup-and-installation)
3. [Configuration](#configuration)
4. [Running the Project](#running-the-project)
5. [Running Tests](#running-tests)
6. [Key Functionalities](#key-functionalities)
7. [Docker Compose](#docker-compose)

---

## Project Structure

```plaintext
.
├── app
│   ├── __init__.py
│   ├── bank
│   │   ├── account_balances.py   # Logic for calculating account balances
│   │   └── __init__.py
│   ├── database
│   │   ├── __init__.py
│   │   ├── connection.py         # Manages database connections
│   │   ├── dtos.py               # Data Transfer Objects (DTOs)
│   │   ├── mock.py               # Mock data generation
│   │   ├── queries.py            # Query building and execution
│   │   └── schema.py             # Database schema definitions
│   ├── main.py                   # Entry point for the application
│   └── tests
│       ├── __init__.py
│       └── test_database.py      # Unit tests for database functionality
├── poetry.lock
├── pyproject.toml
├── README.md
├── run_tests.sh                  # Script to run tests with the correct environment
└── docker-compose.yml            # Docker Compose configuration
```

---

## Setup and Installation

### Prerequisites

Ensure you have the following installed:

- Python 3.10+
- Poetry (for dependency management)
- Docker (optional, for isolated environments)

### Installation Steps

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. Install dependencies using Poetry:

   ```bash
   poetry install
   ```

3. Activate the Poetry environment:

   ```bash
   poetry shell
   ```

---

## Configuration

### `pyproject.toml`

The `pyproject.toml` file contains configurations for dependencies, testing, and more. Key sections:

#### Dependencies

```toml
[tool.poetry.dependencies]
python = "^3.10"
pytest = "^7.4.0"
black = "^23.9.0"
flake8 = "^6.1.0"
```

#### Pytest Options

```toml
[tool.pytest.ini_options]
minversion = "6.0"
addopts = "-ra -q"
testpaths = [
    "app/tests"
]
pythonpath = [
    "app"
]
```

---

## Running the Project

1. **Run the main application**:

   ```bash
   python app/main.py
   ```

2. **Run tests**:

   ```bash
   ./run_tests.sh
   ```

---

## Running Tests

### Using the Shell Script

A script `run_tests.sh` is provided for convenience:

```bash
#!/bin/bash
PYTHONPATH=app pytest
```

Give it execution permission and run it:

```bash
chmod +x run_tests.sh
./run_tests.sh
```

### Manual Execution

Alternatively, run the tests manually:

```bash
PYTHONPATH=app pytest
```

---

## Key Functionalities

1. **Account Balances**:
   - Logic for calculating monthly balances per account is implemented in `account_balances.py`.

2. **Database Handling**:
   - Connection management is in `connection.py`.
   - Schema definitions are in `schema.py`.
   - Queries are dynamically built using DTOs in `queries.py`.

3. **Mock Data**:
   - `mock.py` contains logic for generating mock data for testing.

4. **Testing**:
   - Tests are located in `app/tests` and use `pytest` for execution.

---

## Docker Compose

The project includes a `docker-compose.yml` file to simplify environment setup and execution. Below is the configuration used:

```yaml
version: '3.9'

services:
  app:
    image: python:3.10.0
    container_name: app-container
    working_dir: /app
    volumes:
      - .:/app
    command: >
      sh -c "poetry install && poetry shell"
    environment:
      - PYTHONUNBUFFERED=1
    tty: true
```

### Using Docker Compose

1. Build and run the container:

   ```bash
   docker-compose up --build
   ```

2. Access the running container shell:

   ```bash
   docker exec -it app-container /bin/bash
   ```

3. Run tests inside the container:

   ```bash
   pytest
   ```

4. Stop and remove the container:

   ```bash
   docker-compose down
   ```

## Enhancing the Modeling/System Orientation

## Modeling Improvements: Using UUIDs as Primary Keys

### Trade-offs of Using UUIDs in Database Design

Using **UUIDs** (Universally Unique Identifiers) as primary keys can bring significant benefits in terms of uniformity and scalability. However, it also introduces challenges, particularly regarding query performance and storage efficiency. Below is a detailed analysis of the trade-offs involved in using UUIDs in the database design.

---

### **Advantages**

1. **Uniformity and Consistency**:
   - Provides a consistent standard for identifiers across all tables.
   - Eliminates the confusion between incremental and composite keys.

2. **Global Uniqueness**:
   - UUIDs are globally unique, allowing seamless integration across distributed systems without risking key collisions.
   - Facilitates data migration and replication between databases.

3. **Scalability in Distributed Systems**:
   - Eliminates the need for centralized coordination to generate unique keys.
   - Ensures scalability across distributed nodes.

4. **Enhanced Security and Privacy**:
   - UUIDs are less predictable than incremental IDs, reducing risks associated with scraping or guessing IDs.
   - Useful for systems exposed to public access, where ID enumeration could be a concern.

---

### **Disadvantages**

1. **Performance Degradation**:
   - **Indexing**: UUIDs require more storage and processing for index operations compared to `INT` or `BIGINT` keys.
   - **Fragmentation**: The pseudo-random nature of UUIDs can lead to index fragmentation, reducing query efficiency.

2. **Query Complexity**:
   - Queries involving UUIDs are harder to read and debug due to their lengthy and non-sequential format.
   - Comparisons of alphanumeric strings are computationally heavier than numeric comparisons.

3. **Increased Storage Requirements**:
   - UUIDs consume more storage space (16 bytes) compared to typical integers (4-8 bytes).
   - Larger indexes can lead to increased memory usage and storage costs.

4. **Query Planning Overhead**:
   - The database query planner may take longer to optimize and execute queries involving UUIDs due to their complexity.

---

### **Best Practices to Mitigate Issues**

1. **Use Time-Ordered UUIDs**:
   - Generate UUIDs in a time-ordered manner (e.g., `UUIDv1`) to reduce index fragmentation and improve insertion performance.
   - **Example**:

     ```sql
     SELECT uuid_generate_v1(); -- PostgreSQL function to generate time-based UUIDs
     ```

2. **Combine with Incremental IDs**:
   - Use UUIDs as secondary keys (`surrogate keys`) while keeping incremental IDs as primary keys for better index performance.

3. **Efficient Storage**:
   - Store UUIDs in a compact binary format (`BINARY(16)` or native `UUID` type) instead of as strings (`VARCHAR(36)`).

4. **Optimized Indexing**:
   - Regularly monitor and optimize indexes to mitigate fragmentation.
   - Use composite indexes for queries involving UUIDs and additional fields.

---

### **Documented Decision for Using UUIDs**

- **Rationale for Using UUIDs**:
  - Global uniqueness and consistency.
  - Scalability for distributed systems.
  - Enhanced privacy and security.

- **Trade-offs**:
  - Potential performance overhead for queries and insertions.
  - Increased storage requirements.

- **Mitigation Actions**:
  - Implement time-ordered UUIDs where possible.
  - Optimize indexes and monitor database performance regularly.

---

## Database Schema Updates

This section outlines the recent updates and improvements made to the database schema. The changes aim to enhance consistency, scalability, and query performance while aligning with best practices for database design.

---

### Key Changes in the Schema

1. **Unified Use of UUIDs**:
   - All tables now use UUIDs as primary keys to ensure global uniqueness and consistency across the schema.
   - This change provides better support for distributed systems and avoids potential conflicts in ID generation.

2. **Introduction of a Generic Transactions Table**:
   - A single `transactions` table replaces separate tables for different types of financial movements (e.g., `transfer_ins`, `transfer_outs`, `pix_movements`).
   - This table uses a `transaction_type` column to differentiate between various types of transactions, improving extensibility and simplifying the schema.
   - **Columns**:
     - `transaction_id`: UUID, primary key.
     - `transaction_type`: Differentiates transaction types (e.g., "transfer_in", "pix_out").
     - `amount`: Stores transaction values as a decimal.
     - `requested_at` and `completed_at`: Timestamps for tracking lifecycle.

3. **Simplification of Temporal Data (d_time)**:
   - The `d_time` table now only includes a UUID `time_id` and `action_timestamp`.
   - This change simplifies time-based queries and removes dependencies on other temporal dimension tables (`d_week`, `d_year`, etc.), which can now be dynamically derived if needed.

4. **Removal of Redundant Data**:
   - `country_name` in the `customers` table was removed as it is derivable from the `customer_city` relationship with the `city` table.
   - This reduces storage requirements and avoids data duplication.

---

### Updated Schema

#### **country**

```sql
CREATE TABLE country (
    country_id UUID PRIMARY KEY,
    country_name VARCHAR(128)
);
```

#### **state**

```sql
CREATE TABLE state (
    state_id UUID PRIMARY KEY,
    state_name VARCHAR(128),
    country_id UUID REFERENCES country(country_id)
);
```

#### **city**

```sql
CREATE TABLE city (
    city_id UUID PRIMARY KEY,
    city_name VARCHAR(256),
    state_id UUID REFERENCES state(state_id)
);
```

#### **customers**

```sql
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    first_name VARCHAR(128),
    last_name VARCHAR(128),
    customer_city UUID REFERENCES city(city_id),
    cpf VARCHAR(11) UNIQUE
);
```

#### **accounts**

```sql
CREATE TABLE accounts (
    account_id UUID PRIMARY KEY,
    customer_id UUID REFERENCES customers(customer_id),
    created_at TIMESTAMP,
    status VARCHAR(128),
    account_branch VARCHAR(128),
    account_check_digit VARCHAR(128),
    account_number VARCHAR(128)
);
```

#### **transactions**

```sql
CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY,
    account_id UUID REFERENCES accounts(account_id),
    amount DECIMAL(18, 2),
    transaction_type VARCHAR(50),
    requested_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR(128)
);
```

#### **d_time**

```sql
CREATE TABLE d_time (
    time_id UUID PRIMARY KEY,
    action_timestamp TIMESTAMP NOT NULL
);
```

---

### Benefits of These Changes

1. **Consistency**:
   - All tables use UUIDs, ensuring uniformity across the schema.

2. **Extensibility**:
   - The generic `transactions` table simplifies schema evolution by supporting new transaction types without requiring new tables.

3. **Simplified Queries**:
   - Temporal queries are streamlined by reducing dependencies on complex dimensional tables.

4. **Improved Data Integrity**:
   - Removing redundant data minimizes risks of inconsistencies and optimizes storage usage.

---

### Materialized Views

#### Overview

Materialized views are introduced to optimize the performance of frequently executed queries by precomputing and storing their results. These views enable efficient reporting and aggregation, especially in a transactional context.

---

#### Implemented Materialized Views

### Materialized Views

#### Overview

Materialized views are introduced to optimize the performance of frequently executed queries by precomputing and storing their results. These views enable efficient reporting and aggregation, especially in a transactional context.

---

#### Implemented Materialized Views

1. **Monthly Account Balances**
   - **Description**: Summarizes the net balance of each account on a monthly basis by aggregating transactions.
   - **Query**:

     ```sql
     CREATE MATERIALIZED VIEW monthly_account_balances AS
     SELECT
         accounts.account_id,
         DATE_TRUNC('month', transactions.requested_at) AS action_month,
         SUM(CASE WHEN transactions.transaction_type = 'transfer_in' THEN transactions.amount ELSE 0 END) -
         SUM(CASE WHEN transactions.transaction_type = 'transfer_out' THEN transactions.amount ELSE 0 END) AS net_balance
     FROM accounts
     LEFT JOIN transactions ON accounts.account_id = transactions.account_id
     GROUP BY accounts.account_id, DATE_TRUNC('month', transactions.requested_at);
     ```

2. **Daily Transactions Report**
   - **Description**: Provides a summary of all transactions, grouped by transaction type and date.
   - **Query**:

     ```sql
     CREATE MATERIALIZED VIEW daily_transactions_report AS
     SELECT
         DATE(transactions.requested_at) AS transaction_date,
         SUM(CASE WHEN transactions.transaction_type = 'transfer_in' THEN transactions.amount ELSE 0 END) AS total_transfer_in,
         SUM(CASE WHEN transactions.transaction_type = 'transfer_out' THEN transactions.amount ELSE 0 END) AS total_transfer_out,
         SUM(CASE WHEN transactions.transaction_type = 'pix_in' THEN transactions.amount ELSE 0 END) AS total_pix_in,
         SUM(CASE WHEN transactions.transaction_type = 'pix_out' THEN transactions.amount ELSE 0 END) AS total_pix_out
     FROM transactions
     GROUP BY DATE(transactions.requested_at);
     ```

3. **Customer Financial Overview**
   - **Description**: Aggregates the financial activities of customers, including transaction counts and balances for each type of transaction.
   - **Query**:

     ```sql
     CREATE MATERIALIZED VIEW customer_financial_overview AS
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
     ```

4. **Top Performing Accounts**
   - **Description**: Identifies the top accounts based on their total incoming transactions.
   - **Query**:

     ```sql
     CREATE MATERIALIZED VIEW top_performing_accounts AS
     SELECT
         accounts.account_id,
         SUM(CASE WHEN transactions.transaction_type = 'transfer_in' THEN transactions.amount ELSE 0 END) AS total_transfer_in,
         SUM(CASE WHEN transactions.transaction_type = 'pix_in' THEN transactions.amount ELSE 0 END) AS total_pix_in,
         SUM(CASE WHEN transactions.transaction_type IN ('transfer_in', 'pix_in') THEN transactions.amount ELSE 0 END) AS total_incoming,
         RANK() OVER (ORDER BY SUM(CASE WHEN transactions.transaction_type IN ('transfer_in', 'pix_in') THEN transactions.amount ELSE 0 END) DESC) AS rank
     FROM accounts
     LEFT JOIN transactions ON accounts.account_id = transactions.account_id
     GROUP BY accounts.account_id;
     ```

---

#### Advantages of Materialized Views

1. **Performance Boost**:
   - Precomputed results reduce the overhead of frequently executed complex queries.

2. **Scalability**:
   - Efficient for large datasets as heavy computations are done once and reused.

3. **Ease of Reporting**:
   - Simplifies the process of generating reports by providing ready-to-use summaries.

Materialized views in the new schema align with the goal of simplifying the data model while maintaining performance and scalability.

### Materialized Views: Old Schema vs. New Schema

#### Overview

The materialized views implemented in this project are optimized for the **new schema**, which simplifies relationships and consolidates transactions into a single table. This section explains how these views align with the new schema and highlights the challenges of adapting them to the old schema.

---

#### Key Changes in the New Schema

1. **Consolidated Transactions Table**:
   - All transaction types (`transfer_in`, `transfer_out`, `pix_in`, `pix_out`) are stored in a single `transactions` table with a `transaction_type` column.
   - Simplifies queries by avoiding the need to join multiple transaction-specific tables.

2. **Simplified Relationships**:
   - Intermediate dimension tables like `d_time`, `d_week`, and `d_month` have been removed.
   - Timestamps (`requested_at`, `completed_at`) are stored directly in the `transactions` table, reducing complexity.

3. **UUIDs for Consistency**:
   - UUIDs are used for all primary keys, ensuring global uniqueness and facilitating integration across distributed systems.

---

#### Benefits of the New Schema for Materialized Views

1. **Monthly Account Balances**:
   - Aggregates transaction data by month directly using the `requested_at` column in the `transactions` table.
   - Eliminates the need to join with a `d_month` table.

2. **Daily Transactions Report**:
   - Groups transactions by day using the `requested_at` column.
   - Simplified aggregation without requiring joins with `d_time` or `d_week`.

3. **Customer Financial Overview**:
   - Combines `customers`, `accounts`, and `transactions` for a holistic view of customer activity.
   - The `transaction_type` column allows for straightforward aggregation of transaction amounts.

4. **Top Performing Accounts**:
   - Ranks accounts based on total incoming transactions (`transfer_in` and `pix_in`).
   - Directly calculates rankings using the consolidated `transactions` table.

---

#### Challenges with the Old Schema

If these views were implemented in the old schema, the following challenges would arise:

1. **Separate Transaction Tables**:
   - Queries would need to join multiple transaction-specific tables (`transfer_ins`, `transfer_outs`, `pix_movements`), increasing complexity.
   - Example:
     ```sql
     LEFT JOIN transfer_ins ON accounts.account_id = transfer_ins.account_id
     LEFT JOIN transfer_outs ON accounts.account_id = transfer_outs.account_id
     LEFT JOIN pix_movements ON accounts.account_id = pix_movements.account_id
     ```

2. **Dimension Tables**:
   - Time-based aggregations would require joins with `d_time`, `d_week`, and `d_month`.
   - Example:
     ```sql
     LEFT JOIN d_time ON transfer_ins.transaction_requested_at = d_time.time_id
     LEFT JOIN d_week ON d_time.week_id = d_week.week_id
     ```

3. **Performance Overhead**:
   - The additional joins and lookups across multiple tables would degrade query performance.
   - Storing redundant data in dimension tables would increase storage requirements.

4. **Maintenance Complexity**:
   - Changes to dimension tables would require updating multiple views, making maintenance more challenging.

---

#### Conclusion

The new schema offers significant advantages for materialized views:
- **Simpler Queries**: Direct relationships and a unified `transactions` table reduce complexity.
- **Better Performance**: Eliminating intermediate tables improves query execution time.
- **Easier Maintenance**: The streamlined schema simplifies updates and extensions.

If necessary, the views can be adapted to the old schema, but the new schema is strongly recommended for its simplicity, scalability, and efficiency.

---
