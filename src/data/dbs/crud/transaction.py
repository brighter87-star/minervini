from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session
from src.data.dbs.models.transaction import Base, StockTransaction
from src.data.dbs.session import Database


class CRUDTransaction:
    def __init__(self, session: Session):
        self.session = session

    def create(
        self,
        T: str,
        price: float,
        quantity: int,
        fee: float,
        tax: float,
        tr_type: str,
        tr_date_us: str,
    ):
        row = {
            "T": T,
            "price": price,
            "fee": fee,
            "tax": tax,
            "tr_type": tr_type,
            "tr_date_us": tr_date_us,
            "quantity": quantity,
        }

        stmt = mysql_insert(StockTransaction).values(row)
        self.session.execute(stmt)

    def read(self):


def main():
    db = Database()
    Base.metadata.create_all(db.engine)
    T = "META"
    fee = 0.5
    tax = 1
    tr_type = "buy"
    price = 121.1
    tr_date_us = "2025-10-10"
    quantity = 42

    with db.session() as s:
        CRUDTransaction(s).create(T, price, quantity, fee, tax, tr_type, tr_date_us)


if __name__ == "__main__":
    main()
