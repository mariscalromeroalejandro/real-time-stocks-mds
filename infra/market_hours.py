from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo


MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = dt_time(hour=9, minute=30)
MARKET_CLOSE = dt_time(hour=16, minute=0)


def market_now() -> datetime:
    return datetime.now(MARKET_TZ)


def is_market_open(now_et: datetime | None = None) -> bool:
    now_et = now_et or market_now()
    if now_et.weekday() > 4:
        return False
    return MARKET_OPEN <= now_et.time() < MARKET_CLOSE


def next_market_open(now_et: datetime | None = None) -> datetime:
    now_et = now_et or market_now()
    current_date = now_et.date()

    if now_et.weekday() > 4:
        days_to_monday = 7 - now_et.weekday()
        next_open_date = current_date + timedelta(days=days_to_monday)
    elif now_et.time() < MARKET_OPEN:
        next_open_date = current_date
    elif now_et.time() >= MARKET_CLOSE:
        next_open_date = current_date + timedelta(days=1)
        while next_open_date.weekday() > 4:
            next_open_date += timedelta(days=1)
    else:
        return now_et

    return datetime.combine(next_open_date, MARKET_OPEN, tzinfo=MARKET_TZ)


def seconds_until_next_market_open(now_et: datetime | None = None) -> int:
    now_et = now_et or market_now()
    if is_market_open(now_et):
        return 0
    return max(1, int((next_market_open(now_et) - now_et).total_seconds()))
