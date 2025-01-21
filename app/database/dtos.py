from abc import ABC, abstractmethod

class BaseDTO(ABC):
    @property
    @abstractmethod
    def table_name(self) -> str:
        """Each DTO must define its corresponding table name"""
        pass

    @property
    @abstractmethod
    def columns(self) -> list[str]:
        """Each DTO must define its corresponding columns"""
        pass

class CountryDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "country"

    @property
    def columns(self) -> list[str]:
        return ["country_id", "country"]

class StateDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "state"

    @property
    def columns(self) -> list[str]:
        return ["state_id", "state", "country_id"]

class CityDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "city"

    @property
    def columns(self) -> list[str]:
        return ["city_id", "city", "state_id"]

class CustomerDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "customers"

    @property
    def columns(self) -> list[str]:
        return ["customer_id", "first_name", "last_name", "customer_city", "country_name", "cpf"]

class AccountDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "accounts"

    @property
    def columns(self) -> list[str]:
        return [
            "account_id",
            "customer_id",
            "created_at",
            "status",
            "account_branch",
            "account_check_digit",
            "account_number",
        ]

class TransferInDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "transfer_ins"

    @property
    def columns(self) -> list[str]:
        return [
            "id",
            "account_id",
            "amount",
            "transaction_requested_at",
            "transaction_completed_at",
            "status",
        ]

class TransferOutDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "transfer_outs"

    @property
    def columns(self) -> list[str]:
        return [
            "id",
            "account_id",
            "amount",
            "transaction_requested_at",
            "transaction_completed_at",
            "status",
        ]

class PixMovementDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "pix_movements"

    @property
    def columns(self) -> list[str]:
        return [
            "id",
            "account_id",
            "in_or_out",
            "pix_amount",
            "pix_requested_at",
            "pix_completed_at",
            "status",
        ]

class MonthDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "d_month"

    @property
    def columns(self) -> list[str]:
        return ["month_id", "action_month"]

class YearDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "d_year"

    @property
    def columns(self) -> list[str]:
        return ["year_id", "action_year"]

class WeekDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "d_week"

    @property
    def columns(self) -> list[str]:
        return ["week_id", "action_week"]

class WeekdayDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "d_weekday"

    @property
    def columns(self) -> list[str]:
        return ["weekday_id", "action_weekday"]

class TimeDTO(BaseDTO):
    @property
    def table_name(self) -> str:
        return "d_time"

    @property
    def columns(self) -> list[str]:
        return [
            "time_id",
            "action_timestamp",
            "week_id",
            "month_id",
            "year_id",
            "weekday_id",
        ]
