from dataclasses import dataclass

@dataclass
class CountryDTO:
    table_name: str = "country"
    columns: list[str] = ["country_id", "country_name"]

@dataclass
class StateDTO:
    table_name: str = "state"
    columns: list[str] = ["state_id", "state_name", "country_id"]

@dataclass
class CityDTO:
    table_name: str = "city"
    columns: list[str] = ["city_id", "city_name", "state_id"]

@dataclass
class CustomerDTO:
    table_name: str = "customers"
    columns: list[str] = ["customer_id", "first_name", "last_name", "customer_city", "cpf"]

@dataclass
class AccountDTO:
    table_name: str = "accounts"
    columns: list[str] = ["account_id", "customer_id", "created_at", "status", "account_branch", "account_check_digit", "account_number"]

@dataclass
class TransactionDTO:
    table_name: str = "transactions"
    columns: list[str] = ["transaction_id", "account_id", "amount", "transaction_type", "requested_at", "completed_at", "status"]
