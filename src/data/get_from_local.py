import logging
from pathlib import Path
from typing import Literal

import pandas as pd
from src.data.save_to_local import save_ticker_overviews_as_parquet
from src.utils.config import OHLC_PATH, TICKER_OVERVIEW_PATH, TICKERLIST_PATH
from src.utils.date import get_datelist, get_this_month_as_string
from src.utils.translate import translate_txt_papago

logger = logging.getLogger(__name__)


def get_tickerlist_from_txt() -> list:
    with Path.open(TICKERLIST_PATH / "tickerlist.txt") as f:
        return [line.strip() for line in f if line.strip()]


def get_ohlc_from_txt(days=200) -> pd.DataFrame:
    datelist = get_datelist(days * 2)
    ohlclist = []
    for date in datelist:
        file_path = OHLC_PATH / f"ohlc_{date}.parquet"

        if not Path.exists(file_path):
            logger.warning("파일 없음: %s", file_path)
            continue

        try:
            ohlclist.append(
                pd.read_parquet(file_path, engine="pyarrow"),
            )
        except Exception as e:
            logger.warning("읽기 실패: %s (%s)", file_path, e, exc_info=False)

    print("txt로부터 ohlc 가져오기 성공!!")
    return pd.concat(ohlclist, ignore_index=False)


def read_ticker_overviews_from_local(lang: Literal["ko", "en"], folder_name="2025-09"):
    """lang에 따라 description 부분이 영어로 되어있거나 파파고 한글 번역된 data를 얻음
    lang = "en" or "ko"
    """
    file_path = TICKER_OVERVIEW_PATH / lang / folder_name / "part-0.parquet"
    return pd.read_parquet(file_path, engine="pyarrow")


def add_description_ko_to_ticker_overview():
    """무작정 실행하면 안됨. 토큰을 아껴야 하기 때문에 이미 존재하는 티커의 경우 하지 않는 것을 고려
    """
    df = read_ticker_overviews_from_local(lang="en")
    df["description_ko"] = df["description"].apply(translate_txt_papago)
    df["snapshot_month"] = get_this_month_as_string()
    save_ticker_overviews_as_parquet(df, subdir="ko")


def main():
    # df = get_ohlc_from_txt(days=10)
    ticker_overview_ko = read_ticker_overviews_from_local(lang="ko")
    ticker_overview_en = read_ticker_overviews_from_local(lang="en")


if __name__ == "__main__":
    main()
