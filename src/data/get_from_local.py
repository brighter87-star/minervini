import logging
from pathlib import Path

import pandas as pd
from src.utils.config import OHLC_PATH, TICKERLIST_PATH
from src.utils.date import get_datelist

logger = logging.getLogger(__name__)


def get_tickerlist_from_txt() -> list:
    with Path.open(TICKERLIST_PATH / "tickerlist.txt") as f:
        return [line.strip() for line in f if line.strip()]


def get_ohlc_from_txt(days=200) -> pd.DataFrame:
    datelist = get_datelist(days + 200)
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

    return pd.concat(ohlclist, ignore_index=False)


def main():
    df = get_ohlc_from_txt(days=10)
    print(df.head(3))


if __name__ == "__main__":
    main()
