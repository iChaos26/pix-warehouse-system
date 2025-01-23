from dataclasses import dataclass, field
from typing import List


@dataclass
class BaseDTO:
    """Base class for all DTOs with common properties."""
    table_name: str
    columns: List[str]


@dataclass
class CountryDTO(BaseDTO):
    table_name: str = "country"
    columns: List[str] = field(default_factory=lambda: ["country_id", "country_name"])


@dataclass
class StateDTO(BaseDTO):
    table_name: str = "state"
    columns: List[str] = field(default_factory=lambda: ["state_id", "state_name", "country_id"])


@dataclass
class CityDTO(BaseDTO):
    table_name: str = "city"
    columns: List[str] = field(default_factory=lambda: ["city_id", "city_name", "state_id"])


@dataclass
class CustomerDTO(BaseDTO):
    table_name: str = "customers"
    columns: List[str] = field(default_factory=lambda: ["customer_id", "first_name", "last_name", "customer_city", "cpf"])


@dataclass
class AccountDTO(BaseDTO):
    table_name: str = "accounts"
    columns: List[str] = field(
        default_factory=lambda: [
            "account_id",
            "customer_id",
            "created_at",
            "status",
            "account_branch",
            "account_check_digit",
            "account_number",
        ]
    )


@dataclass
class TransactionDTO(BaseDTO):
    table_name: str = "transactions"
    columns: List[str] = field(
        default_factory=lambda: [
            "transaction_id",
            "account_id",
            "amount",
            "transaction_type",
            "requested_at",
            "completed_at",
            "status",
        ]
    )


@dataclass
class TransferInDTO(BaseDTO):
    table_name: str = "transfer_ins"
    columns: List[str] = field(
        default_factory=lambda: [
            "id",
            "account_id",
            "amount",
            "transaction_requested_at",
            "transaction_completed_at",
            "status",
        ]
    )


@dataclass
class TransferOutDTO(BaseDTO):
    table_name: str = "transfer_outs"
    columns: List[str] = field(
        default_factory=lambda: [
            "id",
            "account_id",
            "amount",
            "transaction_requested_at",
            "transaction_completed_at",
            "status",
        ]
    )


@dataclass
class PixMovementDTO(BaseDTO):
    table_name: str = "pix_movements"
    columns: List[str] = field(
        default_factory=lambda: [
            "id",
            "account_id",
            "in_or_out",
            "pix_amount",
            "pix_requested_at",
            "pix_completed_at",
            "status",
        ]
    )


@dataclass
class MonthDTO(BaseDTO):
    table_name: str = "d_month"
    columns: List[str] = field(default_factory=lambda: ["month_id", "action_month"])


@dataclass
class YearDTO(BaseDTO):
    table_name: str = "d_year"
    columns: List[str] = field(default_factory=lambda: ["year_id", "action_year"])


@dataclass
class WeekDTO(BaseDTO):
    table_name: str = "d_week"
    columns: List[str] = field(default_factory=lambda: ["week_id", "action_week"])


@dataclass
class WeekdayDTO(BaseDTO):
    table_name: str = "d_weekday"
    columns: List[str] = field(default_factory=lambda: ["weekday_id", "action_weekday"])


@dataclass
class TimeDTO(BaseDTO):
    table_name: str = "d_time"
    columns: List[str] = field(
        default_factory=lambda: [
            "time_id",
            "action_timestamp",
            "week_id",
            "month_id",
            "year_id",
            "weekday_id",
        ]
    )
