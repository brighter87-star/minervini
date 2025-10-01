from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import BIGINT, JSON, Date, Index, Integer, String
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from src.data.dbs.setup_db import ENGINE
from src.data.get_from_local import read_ticker_overviews_from_local


class Base(DeclarativeBase):
    pass


class Ticker_Overview_US(Base):
    """전체 수정해야 함."""

    __tablename__ = "ticker_overview_us"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    T: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        unique=True,
    )  # ticker -> T
    name: Mapped[str] = mapped_column(String(255), nullable=True)
    market: Mapped[str] = mapped_column(String(20), nullable=True)
    locale: Mapped[str] = mapped_column(String(10), nullable=True)
    primary_exchange: Mapped[str] = mapped_column(String(10), nullable=True)
    stock_type: Mapped[str] = mapped_column(String(10), nullable=True)
    active: Mapped[str] = mapped_column(String(10), nullable=True)
    currency_name: Mapped[str] = mapped_column(String(10), nullable=True)
    cik: Mapped[str] = mapped_column(String(10), nullable=True)
    market_cap: Mapped[int] = mapped_column(BIGINT, nullable=True)
    description: Mapped[str] = mapped_column(String(1000), nullable=True)
    homepage_url: Mapped[str] = mapped_column(String(255), nullable=True)
    list_date: Mapped[date] = mapped_column(Date, nullable=True)
    branding: Mapped[dict] = mapped_column(JSON, nullable=True)
    share_class_shares_outstanding: Mapped[int] = mapped_column(BIGINT, nullable=True)
    weighted_shares_outstanding: Mapped[int] = mapped_column(BIGINT, nullable=True)
    round_lot: Mapped[int] = mapped_column(Integer, nullable=True)
    composite_figi: Mapped[str] = mapped_column(String(12), nullable=True)
    share_class_figi: Mapped[str] = mapped_column(String(12), nullable=True)
    phone_number: Mapped[str] = mapped_column(String(20), nullable=True)
    address: Mapped[dict] = mapped_column(JSON, nullable=True)
    sic_code: Mapped[str] = mapped_column(String(4), nullable=True)
    sic_description: Mapped[str] = mapped_column(String(100), nullable=True)
    total_employees: Mapped[int] = mapped_column(BIGINT, nullable=True)
    description_ko: Mapped[str] = mapped_column(String(1000), nullable=True)

    __table_args__ = (
        Index(
            "idx_basic",
            "name",
            "market",
            "locale",
            "primary_exchange",
            "stock_type",
        ),
        Index(
            "list_date",
            "share_class_shares_outstanding",
            "weighted_shares_outstanding",
        ),
        Index("figi_code", "composite_figi", "share_class_figi"),
        Index("idx_sic", "sic_code", "sic_description"),
        Index(
            "ft_description",
            "description",
            mysql_prefix="FULLTEXT",
        ),
        Index(
            "ft_description_ko",
            "description_ko",
            mysql_prefix="FULLTEXT",
        ),
    )


def init_schema():
    Base.metadata.create_all(ENGINE)
    print("Schema is Ready!!!")


def save_upsert(
    df,
    *,
    mapping: Mapping[str:str] | None = None,
    chunk_size: int = 1000,
) -> int:
    """ "ticker","""
    default_mapping = {
        "T": "ticker",
        "name": "name",
        "market": "market",
        "locale": "locale",
        "primary_exchange": "primary_exchange",
        "stock_type": "type",
        "active": "active",
        "currency_name": "currency_name",
        "cik": "cik",
        "market_cap": "market_cap",
        "description": "description",
        "homepage_url": "homepage_url",
        "list_date": "list_date",
        "branding": "branding",
        "share_class_shares_outstanding": "share_class_shares_outstanding",
        "weighted_shares_outstanding": "weighted_shares_outstanding",
        "round_lot": "round_lot",
        "composite_figi": "composite_figi",
        "share_class_figi": "share_class_figi",
        "phone_number": "phone_number",
        "address": "address",
        "sic_code": "sic_code",
        "sic_description": "sic_description",
        "total_employees": "total_employees",
        "description_ko": "description_ko",
    }
    if mapping:
        default_mapping.update(mapping)
    mp = default_mapping
    cols = default_mapping.values()
    df = df[cols].copy()

    df["list_date"] = pd.to_datetime(df["list_date"]).dt.date
    df = df.replace({np.nan: None})

    rows = (
        df[list(mp.values())]
        .rename(columns={v: k for k, v in mp.items()})
        .to_dict(orient="records")
    )

    with Session(ENGINE) as s, s.begin():
        for i in range(0, len(rows), chunk_size):
            batch = rows[i : i + chunk_size]
            stmt = mysql_insert(Ticker_Overview_US).values(batch)

            update_cols = {
                c.name: stmt.inserted[c.name]
                for c in Ticker_Overview_US.__table__.columns
                if c.name not in ("id",)
            }
            stmt = stmt.on_duplicate_key_update(**update_cols)

            s.execute(stmt)

    return len(rows)


def post_ohlc_from_local():
    pass


def main():
    df = read_ticker_overviews_from_local(lang="ko")
    init_schema()
    try:
        save_upsert(df)
    except IntegrityError as e:
        print("IntegrityError:", e.orig)  # MySQL/MariaDB의 구체 메시지
        raise


if __name__ == "__main__":
    main()
