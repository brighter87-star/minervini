import logging
import os
import re
from pathlib import Path

import pandas as pd
import requests
from src.data.get_from_local import get_tickerlist_from_txt
from src.utils.config import (
    NASDAQURL,
    OHLC_PATH,
    OTHERURL,
    POLYGON_APIKEY,
    POLYGON_OHLC_URL,
    TICKERLIST_PATH,
)
from src.utils.date import get_datelist


def _get_tickerlist_from_web(url) -> pd.DataFrame:
    df = pd.read_csv(url, sep="|")
    df.rename(columns={"Symbol": "Ticker", "ACT Symbol": "Ticker"}, inplace=True)
    return df


def write_tickerslist():
    N_df = _get_tickerlist_from_web(NASDAQURL)
    O_df = _get_tickerlist_from_web(OTHERURL)
    N_filtered_df = _filter_common_stocks_only(N_df)
    O_filtered_df = _filter_common_stocks_only(O_df)
    tickers_only = [
        str(t).strip()
        for t in list(set(N_filtered_df["Ticker"]) | set(O_filtered_df["Ticker"]))
        if pd.notna(t)
    ]
    with open(TICKERLIST_PATH / "tickerlist.txt", "w") as f:
        f.writelines(ticker + "\n" for ticker in tickers_only)


def _filter_common_stocks_only(df):
    whitelist = ["QQQ"]
    exclude_keywords = {
        # "ETF",
        # "ETN",
        # "Fund",
        "Trust",
        "Preferred",
        "Warrant",
        "Right",
        "Unit",
        "Note",
        "Bond",
        "Debenture",
        "Depositary",
    }
    pattern = r"\b(?:" + "|".join(map(re.escape, exclude_keywords)) + r")\b"
    mask_ex = df["Security Name"].str.contains(
        pattern,
        case=False,
        na=False,
        regex=True,
    )

    return df.loc[~mask_ex | df["Ticker"].isin(whitelist)]


def get_ohlc_all(tickers, interval="day", date="2025-09-11") -> pd.DataFrame:
    response = requests.get(
        url=f"{POLYGON_OHLC_URL}/{date}?adjusted=true&apikey={POLYGON_APIKEY}",
    )
    if response.status_code != 200:
        logging.error("API 요청 실패")
        return None

    results_df = pd.DataFrame(response.json()["results"])
    filtered_df = results_df[results_df["T"].isin(tickers)]
    return filtered_df


def get_ohlc_all_from_web(days=30, interval="day"):
    """전 종목의 ohlc 데이터를 가져올 때 main에서 사용."""
    tickers = get_tickerlist_from_txt()
    date_list = get_datelist(days=days)
    os.makedirs(OHLC_PATH, exist_ok=True)
    api_call_cnt = 0

    for idx, date in enumerate(date_list):
        try:
            file_path = OHLC_PATH / f"ohlc_{date}.parquet"
            if Path.exists(file_path):
                print(f"{file_path}는 이미 존재하므로 건너뛰고 진행합니다.\n")
                continue
        except Exception:
            raise Exception("파일 존재 여부를 확인하는 과정에서 문제가 생겼어요.")

        try:
            print(f"{date}의 데이터를 얻어옵니다.{idx+1}/{len(date_list)}")
            api_call_cnt += 1
            df = get_ohlc_all(tickers, date=date)
        except Exception:
            raise Exception("API call의 과정에서 문제가 생겼어요.")

        if df is not None:
            df["date"] = pd.to_datetime(date)
            df.to_parquet(file_path, index=False)
        else:
            logging.warning("데이터 없음")

        """
        if api_call_cnt % 5 == 0:
            time.sleep(60)
            print("\nAPI 제한 때문에 조금 쉬는 중이에요...")
        """

    print("***********OHLC 데이터 가져오기 종료.***********")


def main():
    # get_ixic_index_from_web(days=3)
    write_tickerslist()
    get_ohlc_all_from_web(days=900)


if __name__ == "__main__":
    main()
