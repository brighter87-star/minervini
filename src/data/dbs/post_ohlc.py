from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import BIGINT, DECIMAL, Date, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from src.data.dbs.session import Database
from src.data.get_from_local import get_ohlc_from_txt


class Base(DeclarativeBase):
    pass


class OHLC_US(Base):
    __tablename__ = "ohlc_us"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    T: Mapped[str] = mapped_column(String(10), nullable=False)
    close_date: Mapped[date] = mapped_column(Date, nullable=True)
    open: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    close: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    volume: Mapped[int] = mapped_column(BIGINT, nullable=True)
    high: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    low: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    vw: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    n: Mapped[int] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("T", "close_date", name="uq_T_date"),
        Index("T", "close_date", "close", "volume", "vw", "n"),
    )


def init_schema():
    Base.metadata.create_all(Database().engine)
    print("Schema is Ready!!!")


def save_upsert(
    df,
    *,
    mapping: Mapping[str:str] | None = None,
    chunk_size: int = 500,
) -> int:
    default_mapping = {
        "T": "T",
        "close_date": "date",
        "open": "o",
        "high": "h",
        "low": "l",
        "close": "c",
        "volume": "v",
        "vw": "vw",
        "n": "n",
    }
    if mapping:
        default_mapping.update(mapping)
    mp = default_mapping
    cols = default_mapping.values()
    df = df[cols].copy()

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.replace({np.nan: None})

    rows = (
        df[list(mp.values())]
        .rename(columns={v: k for k, v in mp.items()})
        .to_dict(orient="records")
    )

    with Database().session() as s:
        for i in range(0, len(rows), chunk_size):
            batch = rows[i : i + chunk_size]
            stmt = mysql_insert(OHLC_US).values(batch)

            update_cols = {
                c.name: stmt.inserted[c.name]
                for c in OHLC_US.__table__.columns
                if c.name not in ("id",)
            }
            stmt = stmt.on_duplicate_key_update(**update_cols)

            s.execute(stmt)

    return len(rows)


def main():
    df = get_ohlc_from_txt(days=200)
    init_schema()
    save_upsert(df)


if __name__ == "__main__":
    main()
