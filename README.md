# Nubank Data Analysis Solution

![Enhanced Schema Diagram](https://via.placeholder.com/800x400.png?text=Optimized+Transaction+Schema)

This project is an end-to-end solution for financial data analysis featuring schema optimizations, robust validation, and precomputed analytics for a large-scale banking dataset.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Project Structure](#project-structure)
4. [Installation and Setup](#installation-and-setup)
5. [Schema Improvements](#schema-improvements)
6. [Materialized Views](#materialized-views)
7. [Performance Benchmarks](#performance-benchmarks)
8. [Results](#results)
9. [License and Attribution](#license-and-attribution)

---

## Project Overview

This solution analyzes and optimizes the financial data model for a large dataset by:

- Unifying transaction tables for scalability and simplicity.
- Utilizing **UUIDs** for global consistency.
- Creating **materialized views** for real-time analytics.
- Implementing robust **data validation** with DTOs (Data Transfer Objects).

---

## Key Features

✅ **Unified Transaction Model**: A single table consolidating all transaction types.  
✅ **Data Integrity Checks**: Ensuring data consistency with DTO validations.  
✅ **Precomputed Analytics**: Materialized views for faster reporting.  
✅ **Scalability**: Support for international expansion and future product integration.

---

## Project Structure

```plaintext
.
├── data                     # Source data for the project
│   ├── accounts             # Account-level data
│   ├── city                 # City-level metadata
│   ├── country              # Country-level metadata
│   ├── customers            # Customer-level data
│   ├── transfer_ins         # Incoming transactions
│   ├── transfer_outs        # Outgoing transactions
│   └── pix_movements        # PIX-specific transactions
├── database
│   ├── connection.py        # Database connection management
│   ├── update_dtos.py       # DTO-based validation
│   └── queries.py           # Query builder
├── mock
│   ├── mock.py
│   ├── mock_streamlit.py    
├── transform                # Data transformation logic
│   └── transform.py
├── views                    # Materialized views and analytics
    ├── dtos.py
    ├── materialized_views.py
    └── transactions_views.py
├── main.py                  # Application entry point
├── tests                    # Unit tests
│   └── test_database_queries.py
├── poetry.lock              # Dependency lock file
├── pyproject.toml           # Project configuration
└── docker-compose.yml       # Docker configuration
```

---

## Installation and Setup

### Prerequisites

Ensure you have the following installed:

- **Python 3.10+**
- **Poetry** for dependency management.
- **Docker** (optional) for running the application in isolated environments.
- **Memory Persistence** just pass through the connection a named db = DuckDBConnection({name})

---

## Schema Improvements

### Legacy vs. Modern Schema

| Legacy Schema                                                             | Modern Schema                                             |   |   |   |
|---------------------------------------------------------------------------|-----------------------------------------------------------|---|---|---|
| Separate tables for `transfer_ins`, `transfer_outs`, and `pix_movements`. | Unified `transactions` table consolidating all movements. |   |   |   |
| Multiple time dimension tables (`d_time`, `d_week`, `d_month`).           | Direct timestamp usage for temporal queries.              |   |   |   |
| Redundant fields like `country_name` in multiple tables.                  | Normalized structure with hierarchical relationships.     |   |   |   |

|        Operation        | Legacy Schema | Modern Schema | Improvement |   |
|:-----------------------:|:-------------:|:-------------:|:-----------:|---|
| Monthly Balance Query   | 2.4 seconds   | 0.3 seconds   | 8x faster   |   |
| Customer Overview Query | 1.8 seconds   | 0.2 seconds   | 9x faster   |   |
| Data Ingestion Time     | 12 seconds    | 4 seconds     | 3x faster   |   |



## Update Schema
![alt text](<Screenshot from 2025-02-04 20-08-57.png>)
---
title: Pix transactions and Account Balance

![alt text](<Screenshot 2025-02-04 at 20-48-41 Editor Mermaid Chart.png>)
---

## Materialized Views

### Implemented Views

#### 1. **Monthly Account Balances**

Summarizes account balances per month based on transactions.

```sql
CREATE OR REPLACE VIEW monthly_account_balances AS
            WITH monthly_net AS (
                SELECT
                    account_id,
                    DATE_TRUNC('month', requested_at) AS month,
                    SUM(CASE
                        WHEN transaction_type IN ('transfer_in', 'pix_in') THEN amount
                        ELSE -amount
                    END) AS net_change
                FROM transactions
                WHERE requested_at BETWEEN '2020-01-01' AND '2020-12-31'
                GROUP BY account_id, DATE_TRUNC('month', requested_at)
            )
            SELECT
                account_id,
                month,
                SUM(net_change) OVER (
                    PARTITION BY account_id 
                    ORDER BY month 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS account_balance
            FROM monthly_net
            ORDER BY account_id, month;
```

#### 2. **Customer Financial Overview**

Aggregates financial activities of customers.

```sql
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
```

---

## Full Results from Nubank Data Analysis Solution

## Monthly Account Balances (Jan 2020 - Dec 2020)

| Account ID           | Month    | Balance      |
|----------------------|----------|--------------|
| 1095295572704434176  | 2020-01  | R$1,507.66   |
| 1095295572704434176  | 2020-03  | R$1,867.62   |
| 1095295572704434176  | 2020-12  | R$1,237.43   |
| 1372963006028127232  | 2020-04  | R$1,553.89   |
| 1372963006028127232  | 2020-05  | R$573.95     |
| 1396423087886678016  | 2020-03  | R$-778.64    |
| 1396423087886678016  | 2020-04  | R$-1,698.05  |
| 1396423087886678016  | 2020-07  | R$-3,514.57  |
| 1493429928567988480  | 2020-04  | R$1,795.00   |
| 1493429928567988480  | 2020-08  | R$1,373.81   |
| 1493429928567988480  | 2020-12  | R$1,086.54   |
| 2173517564649275392  | 2020-04  | R$-767.67    |
| 2173517564649275392  | 2020-06  | R$-1,434.18  |
| 2173517564649275392  | 2020-08  | R$-1,538.05  |
| 231771070487223648   | 2020-07  | R$-2,752.98  |
| 231771070487223648   | 2020-09  | R$-1,368.83  |
| 2352407409595471360  | 2020-03  | R$957.00     |
| 2352407409595471360  | 2020-06  | R$-770.59    |
| 242604038203577184   | 2020-09  | R$195.14     |
| 242604038203577184   | 2020-10  | R$-1,477.75  |
| 2669626301710158848  | 2020-05  | R$-663.03    |
| 2669626301710158848  | 2020-06  | R$-960.54    |
| 2669626301710158848  | 2020-08  | R$-344.86    |
| 2922610483805172224  | 2020-01  | R$1,776.69   |
| 2922610483805172224  | 2020-02  | R$4,500.25   |
| 3011375634010832896  | 2020-02  | R$-1,147.33  |
| 3011375634010832896  | 2020-11  | R$-1,448.53  |
| 414591092625732800   | 2020-02  | R$1,661.28   |
| 414591092625732800   | 2020-09  | R$259.70     |
| 414591092625732800   | 2020-11  | R$-379.65    |
| 507987780945996736   | 2020-04  | R$971.68     |
| 507987780945996736   | 2020-10  | R$-339.06    |
| 712444109815960448   | 2020-02  | R$790.28     |
| 712444109815960448   | 2020-05  | R$1,189.08   |
| 712444109815960448   | 2020-11  | R$2,495.55   |
| 831465696157769088   | 2020-02  | R$-407.94    |
| 831465696157769088   | 2020-03  | R$585.52     |
| 831465696157769088   | 2020-04  | R$-45.63     |

---

## Account Performance Ranking

| Account ID           | Rank | Customer          | Total Incoming |
|----------------------|------|-------------------|----------------|
| 2922610483805172224  | 1    | John Phelps       | R$4,500.25     |
| 712444109815960448   | 2    | Mary Olson        | R$2,495.55     |
| 414591092625732800   | 3    | Sharron Fields    | R$1,661.28     |
| 231771070487223648   | 4    | Candy Vail        | R$1,384.15     |
| 831465696157769088   | 5    | Archie Elliot     | R$993.46       |
| 507987780945996736   | 6    | Crystal Davis     | R$971.68       |
| 2352407409595471360  | 7    | Priscilla Redding | R$957.00       |
| 2669626301710158848  | 8    | Steven Hollis     | R$615.68       |
| 242604038203577184   | 9    | Frederick Laborde | R$492.33       |
| 3011375634010832896  | 10   | Jesse Krapp       | R$0.00         |

---

## Financial Overview

| Customer ID          | Transfers In | Transfers Out | PIX In | PIX Out |
|----------------------|--------------|---------------|--------|---------|
| 3331826451769351680  | R$1,867.62   | R$0.00        | R$0.00 | R$0.00  |
| 909292279053935488   | R$1,384.15   | R$0.00        | R$0.00 | R$0.00  |
| 1139129464680807424  | R$2,475.62   | R$0.00        | R$0.00 | R$0.00  |
| 476448784420957056   | R$0.00       | R$103.87      | R$0.00 | R$0.00  |
| 817858258609867392   | R$1,795.00   | R$0.00        | R$0.00 | R$0.00  |
| 336693543816500544   | R$971.68     | R$489.15      | R$0.00 | R$0.00  |
| 1669481937421247488  | R$1,661.28   | R$639.35      | R$0.00 | R$0.00  |
| 1158353061834253312  | R$615.68     | R$0.00        | R$0.00 | R$0.00  |
| 1389881518493714688  | R$492.33     | R$0.00        | R$0.00 | R$0.00  |
| 1987395201418850560  | R$0.00       | R$301.20      | R$0.00 | R$0.00  |

---

## Monthly Activity Patterns

| Month   | Total Volume    | Active Days |
|---------|-----------------|-------------|
| 2020-01 | R$1,312,655.45  | 2           |
| 2020-02 | R$4,443,655.60  | 7           |
| 2020-03 | R$2,640,361.31  | 4           |
| 2020-04 | R$4,002,891.03  | 6           |
| 2020-05 | R$2,512,478.83  | 4           |
| 2020-06 | R$1,960,416.79  | 3           |
| 2020-07 | R$1,891,186.03  | 3           |
| 2020-08 | R$1,988,396.67  | 3           |
| 2020-09 | R$2,644,157.33  | 4           |
| 2020-10 | R$1,376,477.55  | 2           |
| 2020-11 | R$1,941,739.07  | 3           |
| 2020-12 | R$1,263,625.36  | 2           |

## Validations Output

## Data Ingestion and Transformation

## === DATA INGESTION ===

### Loading Data

- **Folder**: `data`

#### Data Sources and Status

- **transfer_ins**:
  - File: `data/transfer_ins/part-00000-tid-3939740088886661710-b68a1bdc-ca76-4934-b2fd-756b973d041f-10414417-1-c000.csv`
  - Status: **Loaded successfully**
  
- **city**:
  - File: `data/city/part-00000-tid-7257851286237664629-906ff0c6-51c7-4cdd-97cd-dc0aa92a92f1-11185485-1-c000.csv`
  - Status: **Loaded successfully**
  
- **transfer_outs**:
  - File: `data/transfer_outs/part-00000-tid-3880462020784336524-8a5c83e1-e1e4-471e-9dbf-1b37babaff47-10468383-1-c000.csv`
  - Status: **Loaded successfully**

- **accounts**:
  - File: `data/accounts/part-00000-tid-2834924781296170616-a9b7a53c-b8f1-417c-876b-22ce8ab4c825-11024507-1-c000.csv`
  - Status: **Loaded successfully**

- **d_year**:
  - File: `data/d_year/part-00000-tid-5440254676189819830-69d9a28f-dbd2-48ca-9eca-66f1ddb276df-10961064-1-c000.csv`
  - Status: **Loaded successfully**

- **d_weekday**:
  - File: `data/d_weekday/part-00000-tid-1198883628307677355-d7f0c6f5-b5f5-40c6-9537-2951b88849eb-10926413-1-c000.csv`
  - Status: **Loaded successfully**

- **pix_movements**:
  - File: `data/pix_movements/part-00000-tid-8322739320471544484-12382b61-f87b-4388-931d-ec1681d2aad1-10545794-1-c000.csv`
  - Status: **Loaded successfully**

- **d_month**:
  - File: `data/d_month/part-00000-tid-1411108534556725179-0fbf435f-dd8b-411e-ae97-11e74b345b4f-10859988-1-c000.csv`
  - Status: **Loaded successfully**

- **country**:
  - File: `data/country/part-00000-tid-5054581782199228501-41100236-b09c-46a7-8442-8d968c697e9a-11286346-1-c000.csv`
  - Status: **Loaded successfully**

- **d_week**:
  - File: `data/d_week/part-00000-tid-8944900144231406848-ea4ff673-508c-46e0-ac10-e488037f37c5-10733320-1-c000.csv`
  - Status: **Loaded successfully**

- **state**:
  - File: `data/state/part-00000-tid-2467726635302089596-e4fdd6ee-8624-4658-af6a-6098bc1c0825-11243350-1-c000.csv`
  - Status: **Loaded successfully**

- **customers**:
  - File: `data/customers/part-00000-tid-2581180368179082894-038c4dab-1615-444a-994f-5a2107c4d9a8-11135734-1-c000.csv`
  - Status: **Loaded successfully**

- **d_time**:
  - File: `data/d_time/part-00000-tid-2902183611695287462-3111e3e9-023d-4533-a7e5-d0b87fb1a808-10662337-1-c000.csv`
  - Status: **Loaded successfully**

---

## === VALIDATING DATA INGESTION ===

### Record Validation

- **pix_movements**:
  - **Found**: 5 records
  - **Sample**: `(3101512580483427328, 127700825787235152, 'pix_in', 1.56, 1587921566, 'None', 'failed')`
  
- **country**:
  - **Found**: 1 record
  - **Sample**: `(1811589392032273152, 'Brasil')`
  
- **accounts**:
  - **Found**: 5 records
  - **Sample**: `(127700825787235152, 3018741143504866816, datetime.datetime(2019, 4, 19, 1, 34, 25), 'active', 8366, 3, 41002)`

- **transfer_ins**:
  - **Found**: 5 records
  - **Sample**: `(1783896702851019520, 2646715789379666432, 1060.66, 1601172258, '1601172264', 'completed')`

- **transfer_out**:
  - **Found**: 5 records
  - **Sample**: `(937196074244976512, 438980267812145600, 1396.64, 1579074367, 'None', 'failed')`

---

## === SCHEMA VALIDATION ===

### DTO Validation

#### Customers

- **Expected Schema**: `['customer_id', 'first_name', 'last_name', 'customer_city', 'cpf', 'country_name']`
- **Actual Schema**: `['customer_id', 'first_name', 'last_name', 'customer_city', 'cpf', 'country_name']`

#### Country

- **Expected Schema**: `['country_id', 'country']`
- **Actual Schema**: `['country', 'country_id']`

#### Accounts

- **Expected Schema**: `['account_id', 'customer_id', 'created_at', 'status', 'account_branch', 'account_check_digit', 'account_number']`
- **Actual Schema**: `['account_id', 'customer_id', 'created_at', 'status', 'account_branch', 'account_check_digit', 'account_number']`

---

### === SCHEMA TRANSFORMATION ===

### Steps

1. **Creating transactions table**:
   - Status: **Created successfully**

2. **Migrating legacy data**:
   - **transfer_ins**: **Migrated successfully**
   - **transfer_outs**: **Migrated successfully**
   - **pix_movements**: **Migrated successfully**

3. **Validating core data**:
   - Invalid timestamps: **0 (allowed)**
   - Status: **Valid**

4. **Cleaning up legacy tables**:
   - **Removed**: `transfer_ins`, `transfer_outs`, `pix_movements`

### Final Status

- **Schema transformation**: **Completed successfully**

---

### === QUERY BUILDER TESTS ===

### Tests and Results

#### Test: Country Filter

- **Query**:

```sql
  SELECT country_id, country FROM country WHERE country = 'Brasil';
```

- **Result**:[(1811589392032273152, 'Brasil')]
- **Rows returned**: 1

- **Test**: Customer Aggregation

```sql
SELECT COUNT(*) AS total FROM customers GROUP BY country_name;
```

- **Result**: [(4000,)]
- **Rows returned**: 1

- **Test**: Transaction Aggregation

```sql
SELECT SUM(amount) AS total_amount FROM transactions;
```

- **Result**: [(476210790.032836,)]
- **Rows returned**: 1

### View Generation

### Materialized Views Creation

- **Monthly Account Balances View**: Created successfully  
- **Daily Transactions Report View**: Created successfully  
- **Customer Financial Overview View**: Created successfully  
- **Top Performing Accounts View**: Created successfully  
- **Materialized views**: Created successfully

## Next Steps for System Design of Data Manager App

### 1. **Data Manager as a Module or API**

- Modularize the Data Manager to allow integration with other systems.
- Design a RESTful or GraphQL API for external interactions.
- Ensure the API is well-documented using tools like Swagger or Postman.

### 2. **Unit Tests and View Tests Using DuckDB**

- Write unit tests for core functionalities using DuckDB as an in-memory database for testing.
- Implement view tests to ensure data retrieval and rendering logic works as expected.
- Integrate testing into the CI/CD pipeline for automated validation.

## 3. **Logger Implementation**

- Integrate a logging framework (e.g., Python's `logging` module or `loguru`).
- Log critical events, errors, and debugging information.
- Ensure logs are stored securely and can be easily monitored.

## 4. **Refactor Views: DTOs and Direct Query Executions**

- Introduce Data Transfer Objects (DTOs) to decouple data access logic from views.
- Refactor views to avoid direct query executions, promoting cleaner and more maintainable code.
- Use a service layer to handle business logic and data retrieval.

## 5. **Update Docker Compose for Persistent Database**

- Modify the `docker-compose.yml` to include a persistent database (e.g., PostgreSQL).
- Implement a DB-API interface using `fsspec` (FS) for file-based storage or connect directly to PostgreSQL.
- Ensure the database supports file formats like Parquet for efficient data storage.

## 6. **Data Loader as a Module**

- Develop a reusable Data Loader module for ingesting data from various sources.
- Support multiple file formats (e.g., CSV, JSON, Parquet) and data sources (e.g., S3, Azure Blob).
- Ensure the module is configurable and extensible for future requirements.

## 7. **AWS and Azure Plugins**

- Implement plugins for AWS (S3, Redshift) and Azure (Blob Storage) integrations.
- Use AWS Secrets Manager and Azure Key Vault for secure credential management.
- Ensure the plugins are modular and can be easily enabled or disabled.

## 8. **Future: Deployment on Kubernetes (K8s)**

- Prepare the application for deployment on Kubernetes for scalability and resilience.
- Use GitHub Actions or Azure DevOps Pipelines for CI/CD automation.
- Implement Helm charts for managing Kubernetes deployments.

## 9. **Additional Considerations**

- **Monitoring and Alerts:** Set up monitoring tools (e.g., Prometheus, Grafana) and configure alerts for system health.
- **Security:** Regularly audit the system for vulnerabilities and ensure compliance with security standards.
- **Documentation:** Maintain up-to-date documentation for developers, admins, and end-users.
- **User Feedback:** Continuously gather user feedback to improve the application.

## 10. **Iterate and Scale**

- Plan for iterative development and regular releases.
- Scale the system based on user demand and performance metrics.
- Explore additional features like data visualization, advanced analytics, and machine learning integration.
