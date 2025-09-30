import inspect

import numpy as np
import pandas as pd
from src.data.get_from_local import get_ohlc_from_txt


class ModificationError(Exception):
    def __init__(self, message: str):
        frame = inspect.stack()[1]
        func_name = frame.function
        super().__init__(f"[{func_name}] {message}")


def add_50_150_200_ma(df: pd.DataFrame) -> pd.DataFrame:
    try:
        print("데이터프레임에 이평선을 추가하는 중...")
        df = df.sort_values(by=["T", "date"]).copy()
        g = df.groupby(by=["T"])["c"]
        for w in (50, 150, 200):
            df[f"ma{w}"] = g.transform(
                lambda x: x.rolling(window=w).mean(),
            )
        return df
    except Exception as e:
        raise ModificationError(f"에러 발생: {e}")


def add_rs_vs_qqq(
    df: pd.DataFrame,
    periods=(50, 150, 200),
    idx_ticker="QQQ",
) -> pd.DataFrame:
    try:
        print("데이터프레임에 QQQ와 비교한 RS를 계산하고 추가하는 중...")
        out = df.sort_values(["T", "date"]).copy()

        idx = (
            out.loc[out["T"] == idx_ticker, ["date", "c"]]
            .sort_values("date")
            .rename(columns={"c": "idx_close"})
        )
        for p in periods:
            idx[f"index_ret_{p}"] = idx["idx_close"] / idx["idx_close"].shift(p)

        out = out.merge(
            idx[["date"] + [f"index_ret_{p}" for p in periods]],
            on="date",
            how="left",
        )

        for p in periods:
            out[f"stock_ret_{p}"] = out.groupby("T")["c"].transform(
                lambda s, p=p: s / s.shift(p),
            )
            num = out[f"stock_ret_{p}"].to_numpy(float)
            den = out[f"index_ret_{p}"].to_numpy(float)
            out[f"RS_{p}"] = np.divide(
                num,
                den,
                out=np.full_like(num, np.nan),
                where=~np.isnan(den) & (den != 0),
            )

        return out
    except Exception as e:
        raise ModificationError(f"에러 발생: {e}")


def add_rs_percentiles_by_date(
    df_rs,
    periods=(50, 150, 200),
    exclude=("QQQ",),
) -> pd.DataFrame:

    try:
        print("데이터프레임에 RS percentiles 정보를 넣는 중...")
        out = df_rs[~df_rs["T"].isin(exclude)].copy()

        for p in periods:
            out[f"RS_{p}_pct"] = out.groupby("date")[f"RS_{p}"].rank(pct=True)

        cols = [f"RS_{p}_pct" for p in periods]

        out["RS_pct_min"] = out[cols].min(axis=1)
        out["RS_pct_mean"] = out[cols].mean(axis=1)

        return out
    except Exception as e:
        raise ModificationError(f"에러 발생: {e}")


def add_52w_high_52w_low(df: pd.DataFrame) -> pd.DataFrame:

    try:
        print("데이터프레임에 52주 신고가와 신저가 정보를 추가하는 중...")
        df = df.sort_values(["T", df.index.name or "date"]).copy()
        df["52w_high"] = df.groupby("T")["c"].transform(lambda s: s.rolling(252).max())
        df["52w_low"] = df.groupby("T")["c"].transform(lambda s: s.rolling(252).min())
        return df
    except Exception as e:
        raise ModificationError(f"에러 발생: {e}")


def add_is_ma200_up(df: pd.DataFrame, period=20) -> pd.DataFrame:
    try:
        print("데이터프레임에 MA200의 상승 여부 정보를 추가 중...")
        df["is_ma200_up"] = df["ma200"] > df.groupby("T")["ma200"].shift(period)

        return df

    except Exception as e:
        raise ModificationError(f"에러 발생: {e}")


def main():
    df = get_ohlc_from_txt()
    df_ma = add_50_150_200_ma(df)
    df_final = add_rs_vs_qqq(df_ma)


if __name__ == "__main__":
    main()
