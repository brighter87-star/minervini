from sqlalchemy.dialects.mysql import insert as mysql_insert
from src.data.dbs.models.transaction import StockTransaction
from src.data.dbs.session import Database


def main():
    T = "AAPL"
    tr_type = "buy"
    tr_date_us = "2025-10-10"
    quantity = 100

    row = {"T": T, "tr_type": tr_type, "tr_date_us": tr_date_us, "quantity": quantity}

    stmt = mysql_insert(StockTransaction).values(row)

    with Database().session() as s:
        s.execute(stmt)


if __name__ == "__main__":
    main()
