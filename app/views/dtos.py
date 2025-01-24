from dataclasses import dataclass, field
from typing import List
from app.database.update_dtos import BaseDTO
@dataclass 
class AccountBalanceDTO(BaseDTO):
    table_name: str = "monthly_account_balances"
    columns: List[str] = field(default_factory=lambda: [
        "account_id", "month", "net_balance"
    ])