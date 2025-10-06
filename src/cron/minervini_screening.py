from src.analysis.prepare import get_ohlc_ma_rs_for_analysis, save_ohlc_ma_rs_parquet
from src.analysis.screen_minervini import screen_minervini
from src.data.dbs.post_minervini_results import save_upsert as results_upsert
from src.data.dbs.post_ohlc import save_upsert as ohlc_upsert
from src.data.get_from_local import get_ohlc_from_txt
from src.data.get_from_web import get_ohlc_all_from_web, write_tickerslist
from src.utils.date import get_last_business_day


def main():
    """오늘이 공휴일인지 아닌지 확인하고 공휴일이 아닌 경우에만 실행"""
    write_tickerslist()
    get_ohlc_all_from_web(days=7)
    df = get_ohlc_from_txt(days=7)
    ohlc_upsert(df)
    last_business_day = get_last_business_day(as_string=True)
    save_ohlc_ma_rs_parquet(date=last_business_day)
    df = get_ohlc_ma_rs_for_analysis(date=last_business_day)
    result_df = screen_minervini(df, rs_col="RS_200_pct")
    results_upsert(result_df)


if __name__ == "__main__":
    main()
