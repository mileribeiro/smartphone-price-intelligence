from datetime import datetime, timezone
from zoneinfo import ZoneInfo


BUSINESS_TIMEZONE = ZoneInfo("America/Fortaleza")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def business_date(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(BUSINESS_TIMEZONE).date().isoformat()
