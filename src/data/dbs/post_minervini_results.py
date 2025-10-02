from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import BIGINT, DECIMAL, Date, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from src.analysis.prepare import get_ohlc_ma_rs_for_analysis
from src.analysis.screen_minervini import screen_minervini
from src.data.dbs.session import Database
from src.utils.date import get_last_business_day


class Base(DeclarativeBase):
    pass


class MinerviniScreeningResult(Base):
    __tablename__ = "minervini_screening_results"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    T: Mapped[str] = mapped_column(String(10), nullable=False)
    close_date: Mapped[date] = mapped_column(Date, nullable=False)
    c: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    v: Mapped[float] = mapped_column(BIGINT, nullable=True)
    RS_50: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    RS_150: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    RS_200: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    stock_ret_50: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    stock_ret_150: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    stock_ret_200: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    RS_50_pct: Mapped[float] = mapped_column(DECIMAL(10, 6), nullable=True)
    RS_150_pct: Mapped[float] = mapped_column(DECIMAL(10, 6), nullable=True)
    RS_200_pct: Mapped[float] = mapped_column(DECIMAL(10, 6), nullable=True)
    RS_pct_mean: Mapped[float] = mapped_column(DECIMAL(10, 6), nullable=True)
    high_52w: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)
    low_52w: Mapped[float] = mapped_column(DECIMAL(10, 4), nullable=True)

    __table_args__ = (
        UniqueConstraint("T", "close_date", name="uq_T_date"),
        Index("ix_T_v_RS", "T", "v", "RS_50", "RS_150", "RS_200"),
        Index("RS_50_pct", "RS_150_pct", "RS_200_pct", "RS_pct_mean"),
    )


def init_schema():
    Base.metadata.create_all(Database().engine)
    print("Schema Ready!!!")


def save_upsert(df: pd.DataFrame, *, mapping: Mapping[str, str] = None) -> int:
    """DataFrame을 업서트로 저장."""
    default_mapping = {
        "T": "T",
        "close_date": "date",
        "c": "c",
        "v": "v",
        "RS_50": "RS_50",
        "RS_150": "RS_150",
        "RS_200": "RS_200",
        "RS_50_pct": "RS_50_pct",
        "RS_150_pct": "RS_150_pct",
        "RS_200_pct": "RS_200_pct",
        "RS_pct_mean": "RS_pct_mean",
        "stock_ret_50": "stock_ret_50",
        "stock_ret_150": "stock_ret_150",
        "stock_ret_200": "stock_ret_200",
        "high_52w": "52w_high",
        "low_52w": "52w_low",
    }

    cols = default_mapping.values()
    df = df[cols].copy()

    if mapping:
        default_mapping.update(mapping)
    mp = default_mapping

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.replace({np.nan: None})

    with Database().session() as s:
        rows = (
            df[list(mp.values())]
            .rename(columns={v: k for k, v in mp.items()})
            .to_dict(orient="records")
        )
        stmt = mysql_insert(MinerviniScreeningResult).values(rows)

        update_cols = {
            c.name: stmt.inserted[c.name]
            for c in MinerviniScreeningResult.__table__.columns
            if c.name not in ("id",)
        }
        stmt = stmt.on_duplicate_key_update(**update_cols)

        try:
            res = s.execute(stmt)
            print(f"[save_upsert] 실행 OK, rowcount={getattr(res, 'rowcount', 'n/a')}")
        except Exception as e:
            # MySQL 에러 메시지 그대로 보고
            raise RuntimeError(f"[save_upsert] 업서트 실행 실패: {e}") from e

    return len(rows)


def main():
    date = get_last_business_day()
    df = get_ohlc_ma_rs_for_analysis(date=date)
    result_df = screen_minervini(df, rs_col="RS_200_pct")
    init_schema()
    save_upsert(result_df)


if __name__ == "__main__":
    main()
