from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass
class CreateStockTr:
    T: str
    quantity: int
    price: float
    fee: float
    tax: float
    tr_type: Literal["buy", "sell"]
    tr_date_us: str

    def __post_init__(self):
        if self.quantity <= 0 or not isinstance(self.quantity, int):
            raise ValueError("quantity 변수 값이 올바르지 않습니다.")
        if self.price <= 0 or not isinstance(self.price, (int, float)):
            raise ValueError("price 변수 값이 올바르지 않습니다.")
        if self.fee <= 0 or not isinstance(self.fee, (int, float)):
            raise ValueError("fee 변수 값이 올바르지 않습니다.")
        if self.tax <= 0 or not isinstance(self.tax, (int, float)):
            raise ValueError("tax 변수 값이 올바르지 않습니다.")
        if self.tr_type not in ("buy", "sell"):
            raise ValueError("tr_type 값이 올바르지 않습니다.")
        try:
            datetime.strptime(self.tr_date_us, "%Y-%m-%d")
        except ValueError:
            raise ValueError("tr_date_us 값이 올바르지 않습니다.")
