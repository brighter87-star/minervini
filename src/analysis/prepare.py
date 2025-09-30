import logging
from pathlib import Path

import pandas as pd
from src.data.get_from_local import get_ohlc_from_txt
from src.data.preprocessing import (
    add_50_150_200_ma,
    add_52w_high_52w_low,
    add_is_ma200_up,
    add_rs_percentiles_by_date,
    add_rs_vs_qqq,
)
from src.utils.config import OHLC_MA_RS_PATH
from src.utils.date import get_today_as_string


def get_ohlc_ma_rs_for_analysis(date: str, ticker=None) -> pd.DataFrame:
    """여기서 date는 OHLC_MA_RS parquet의 저장 날짜여야 함.
    """
    file_path: Path = OHLC_MA_RS_PATH / f"{date}.parquet"
    df = pd.read_parquet(file_path, engine="pyarrow")
    if ticker:
        return df.loc[df["T"] == ticker]
    return df


def save_ohlc_ma_rs_parquet() -> None:
    """ticker와 기간을 넘겨주면 candle 차트 및 MA를 위한 데이터프레임을 리턴함."""
    df = get_ohlc_from_txt(days=400)
    df = add_50_150_200_ma(df)
    df = add_rs_vs_qqq(df)
    df = add_rs_percentiles_by_date(df)
    df = add_52w_high_52w_low(df)
    df = add_is_ma200_up(df)

    today = get_today_as_string()
    file_path = OHLC_MA_RS_PATH / f"{today}.parquet"
    try:
        print("추가된 OHLC 데이터를 parquet으로 저장하는 중...")
        df.to_parquet(file_path, engine="pyarrow")
    except Exception as e:
        logging.exception(f"{e}, parquet 과정에서 에러 발생")


def main():
    save_ohlc_ma_rs_parquet()


if __name__ == "__main__":
    main()
