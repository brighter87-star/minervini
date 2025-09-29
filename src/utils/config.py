import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
POLYGON_APIKEY = os.getenv("POLYGON_APIKEY")
FIGI_APIKEY = os.getenv("FIGI_APIKEY")
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

# File PATHs
PROJECT_ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = PROJECT_ROOT_PATH / "src"
CACHED_PATH = SRC_PATH / "data" / "cached"
TICKERLIST_PATH = CACHED_PATH / "tickers"
OHLC_PATH = CACHED_PATH / "OHLC"
OHLC_MA_RS_PATH = CACHED_PATH / "OHLC_MA_RS"
TICKER_OVERVIEW_PATH = CACHED_PATH / "ticker_overview"
CSV_ARK_PATH = SRC_PATH / "data" / "cached" / "etf_csv_ark"
CSV_ISHARES_PATH = SRC_PATH / "data" / "cached" / "etf_csv_ishares"
ETF_HOLDINGS_PATH = SRC_PATH / "data" / "cached" / "ETF_Holdings"

# URLs
POLYGON_OHLC_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks"
POLYGON_TICKER_OVERVIEW_URL = "https://api.polygon.io/v3/reference/tickers"
PAPAGO_TRANSLATE_URL = "https://papago.apigw.ntruss.com/nmt/v1/translation"
NASDAQURL = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt"
OTHERURL = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"
SEC_CIK_URL = "https://www.sec.gov/files/company_tickers.json"

if __name__ == "__main__":
    print(POLYGON_APIKEY)
