import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
POLYGON_APIKEY = os.getenv("POLYGON_APIKEY")
FIGI_APIKEY = os.getenv("FIGI_APIKEY")

# File PATHs
PROJECT_ROOT_PATH = Path(__file__).resolve().parent.parent.parent
SRC_PATH = PROJECT_ROOT_PATH / "src"
TICKERLIST_PATH = SRC_PATH / "data" / "cached" / "tickers"
OHLC_PATH = SRC_PATH / "data" / "cached" / "OHLC"
OHLC_MA_RS_PATH = SRC_PATH / "data" / "cached" / "OHLC_MA_RS"
CSV_ARK_PATH = SRC_PATH / "data" / "cached" / "etf_csv_ark"
CSV_ISHARES_PATH = SRC_PATH / "data" / "cached" / "etf_csv_ishares"
ETF_HOLDINGS_PATH = SRC_PATH / "data" / "cached" / "ETF_Holdings"

# URLs
POLYGON_OHLC_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks"
NASDAQURL = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt"
OTHERURL = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"

if __name__ == "__main__":
    print(POLYGON_APIKEY)
