import pandas as pd
import pandas_market_calendars as mcal


def get_datelist(days=0, today=pd.Timestamp.today(tz="UTC")) -> pd.Timestamp:
    nyse = mcal.get_calendar("NYSE")
    day_range = nyse.schedule(
        start_date=today - pd.Timedelta(days=days * 2),
        end_date=today,
    )
    return [_to_string_from_time(date) for date in day_range["market_open"]]


def get_today_as_string():
    return pd.Timestamp.today().strftime("%Y-%m-%d")


def get_this_month_as_string() -> str:
    return pd.Timestamp.today().strftime("%Y-%m")


def _to_string_from_time(date: pd.Timestamp):
    return date.strftime("%Y-%m-%d")


def main():
    print(get_datelist(days=100))


if __name__ == "__main__":
    main()
