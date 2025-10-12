from datetime import date
from enum import Enum

from sqlalchemy import DECIMAL, Date
from sqlalchemy import Enum as SAEnum
from sqlalchemy import Index, Integer, String, UniqueConstraint
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
    price: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    fee: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    tax: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    tr_type: Mapped[TrType] = mapped_column(SAEnum(TrType), nullable=False)
    tr_date_us: Mapped[date] = mapped_column(Date)

    __table_args__ = (
        UniqueConstraint(
            "T", "price", "quantity", "tr_date_us", name="T_price_quantity_tr_date_us"
        ),
        Index("T", "tr_date_us", "price"),
    )
