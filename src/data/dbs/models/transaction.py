from datetime import date
from enum import Enum

from sqlalchemy import Date
from sqlalchemy import Enum as SAEnum
from sqlalchemy import Integer, String, null
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TrType(str, Enum):
    BUY = "buy"
    SELL = "sell"


class StockTransaction(Base):
    __tablename__ = "stock_transaction"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    T: Mapped[str] = mapped_column(String(20), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    tr_type: Mapped[TrType] = mapped_column(SAEnum(TrType), nullable=False)
    tr_date_us: Mapped[date] = mapped_column(Date)
