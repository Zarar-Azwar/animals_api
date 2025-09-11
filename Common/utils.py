from typing import List, Dict, Any, Optional
from datetime import datetime
from dateutil import tz
def normalize_csv_string(csv_string: str) -> List[str]:
    if not csv_string or not csv_string.strip():
        return []
    
    return [item.strip() for item in csv_string.split(',') if item.strip()]


def format_datetime_iso8601(dt: datetime) -> str:
    # Ensure datetime is in UTC
    if dt.tzinfo is None:
        utc_dt = dt.replace(tzinfo=tz.UTC)
    else:
        utc_dt = dt.astimezone(tz.UTC)
    
    return utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')