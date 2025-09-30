import pandas as pd
import pandas_market_calendars as mcal


def get_datelist(
    days=0,
    today=pd.Timestamp.today(tz="America/New_York"),
) -> pd.Timestamp:

    end_date = get_last_business_day()
    print("Date List를 만드는 중...")

    nyse = mcal.get_calendar("NYSE")

    day_range = nyse.schedule(
        start_date=end_date - pd.Timedelta(days=days * 2),
        end_date=end_date,
    )

    return [_to_string_from_time(date) for date in day_range["market_open"]]


def get_last_business_day(as_string=False):
    nyse = mcal.get_calendar(name="NYSE")
    today = pd.Timestamp.today(tz="America/New_York")
    market_close = pd.Timestamp(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=16,
        minute=0,
        tz="America/New_York",
    )
    if today >= market_close:
        end_date = today
    else:
        end_date = today - pd.Timedelta(days=1)

    last_business_day = nyse.schedule(
        start_date=end_date - pd.Timedelta(days=10),
        end_date=end_date,
    ).index[-1]

    if as_string:
        return last_business_day.strftime("%Y-%m-%d")
    return last_business_day


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
