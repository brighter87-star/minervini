import pandas as pd

from src.data.get_from_local import get_ohlc_from_txt
from src.data.preprocessing import (
    add_50_150_200_ma,
    add_rs_percentiles_by_date,
    add_rs_vs_qqq,
)
from src.utils.config import OHLC_MA_RS_PATH
from src.utils.date import get_today_as_string
from pathlib import Path

def get_ohlc_ma_rs_for_analysis(ticker=None) -> pd.DataFrame:
    file_path: Path = OHLC_MA_RS_PATH / "2025-09-26.parquet"
    df = pd.read_parquet(file_path, engine="pyarrow")
    if ticker:
        return df.loc[df["T"] == ticker]
    else :
        return df


def save_ohlc_ma_rs_parquet() -> None:
    """ticker와 기간을 넘겨주면 candle 차트 및 MA를 위한 데이터프레임을 리턴함."""
    df = get_ohlc_from_txt(days=400)
    df = add_50_150_200_ma(df)
    df = add_rs_vs_qqq(df)
    df = add_rs_percentiles_by_date(df)

    today = get_today_as_string()
    file_path = OHLC_MA_RS_PATH / f"{today}.parquet"
    df.to_parquet(file_path, engine="pyarrow")


def main():
    save_ohlc_ma_rs_parquet()


if __name__ == "__main__":
    main()
