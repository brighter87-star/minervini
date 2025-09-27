import numpy as np
import pandas as pd
from src.data.get_from_local import get_ohlc_from_txt


def add_50_150_200_ma(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(by=["T", "date"]).copy()
    g = df.groupby(by=["T"])["c"]
    for w in (50, 150, 200):
        df[f"ma{w}"] = g.transform(
            lambda x: x.rolling(window=w).mean(),
        )

    return df


def add_rs_vs_qqq(
    df: pd.DataFrame,
    periods=(50, 150, 200),
    idx_ticker="QQQ",
) -> pd.DataFrame:
    # df: long ['T','date','c', ...]
    out = df.sort_values(["T", "date"]).copy()

    # 1) 인덱스(QQQ) 전용 DF
    idx = (
        out.loc[out["T"] == idx_ticker, ["date", "c"]]
        .sort_values("date")
        .rename(columns={"c": "idx_close"})
    )
    # 2) 인덱스 리턴을 "인덱스 DF에서" 먼저 계산
    for p in periods:
        idx[f"index_ret_{p}"] = idx["idx_close"] / idx["idx_close"].shift(p)

    # 3) 인덱스 리턴만 종목 DF에 머지 (ffill/bfill 금지; 필요하면 나중에 ffill만)
    out = out.merge(
        idx[["date"] + [f"index_ret_{p}" for p in periods]],
        on="date",
        how="left",
    )

    # 4) 종목(티커별) 리턴
    for p in periods:
        out[f"stock_ret_{p}"] = out.groupby("T")["c"].transform(
            lambda s, p=p: s / s.shift(p),
        )
        # RS = 종목 / 지수
        num = out[f"stock_ret_{p}"].to_numpy(float)
        den = out[f"index_ret_{p}"].to_numpy(float)
        out[f"RS_{p}"] = np.divide(
            num,
            den,
            out=np.full_like(num, np.nan),
            where=~np.isnan(den) & (den != 0),
        )

    return out


def add_rs_percentiles_by_date(df_rs, periods=(50, 150, 200), exclude=("QQQ",)) -> pd.DataFrame:
    out = df_rs[~df_rs["T"].isin(exclude)].copy()

    # 날짜별 퍼센타일(0~1)
    for p in periods:
        out[f"RS_{p}_pct"] = out.groupby("date")[f"RS_{p}"].rank(pct=True)

    cols = [f"RS_{p}_pct" for p in periods]

    # 행 단위 집계(여기서는 groupby 불필요)
    out["RS_pct_min"] = out[cols].min(axis=1)
    out["RS_pct_mean"] = out[cols].mean(axis=1)

    return out


def add_52w_high_52w_low(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["T", df.index.name or "date"]).copy()
    df["52w_high"] = df.groupby("T")["c"].transform(
            lambda s: s.rolling(252).max())
    df["52w_low"] = df.groupby("T")["c"].transform(
            lambda s: s.rolling(252).min())
    return df
    

def add_is_ma200_up(df: pd.DataFrame, period=20) -> pd.DataFrame:
    df["is_ma200_up"] = df["ma200"] > df.groupby("T")["ma200"].shift(period)

    return df


def main():
    df = get_ohlc_from_txt()
    df_ma = add_50_150_200_ma(df)
    df_final = add_rs_vs_qqq(df_ma)


if __name__ == "__main__":
    main()
