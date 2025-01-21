from typing import List, Optional
from database.queries import QueryBuilder
from database.dtos import CountryDTO

class BankAccount:
    def __init__(
        self, account_id: str, customer_id: str, created_at: str, status: str,
        account_branch: str, account_check_digit: str, account_number: str
    ) -> None:
        self.account_id = account_id
        self.customer_id = customer_id
        self.created_at = created_at
        self.status = status
        self.account_branch = account_branch
        self.account_check_digit = account_check_digit
        self.account_number = account_number

class TransactionQuery:
    @staticmethod
    def fetch_transactions_by_account(account_id: str) -> str:
        return QueryBuilder.select_query(
            table_name="transactions", 
            columns=["transaction_id", "amount", "type", "date"], 
            conditions=f"account_id = '{account_id}'"
        )

class AccountBalances:
    @staticmethod
    def calculate_monthly_balance(account_id: str) -> str:
        return (
            "SELECT year, month, SUM(amount) as balance FROM transactions "
            f"WHERE account_id = '{account_id}' GROUP BY year, month ORDER BY year, month;"
        )

class Queries:
    @staticmethod
    def fetch_all_records(dto):
        return QueryBuilder.fetch_all(dto.table_name)

# Example usage
if __name__ == "__main__":
    print(Queries.fetch_all_records(CountryDTO))
    print(AccountBalances.calculate_monthly_balance("some_account_id"))
