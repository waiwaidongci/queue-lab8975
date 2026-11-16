from datetime import datetime, timezone
def now():
    return datetime.now(timezone.utc).isoformat()
def now_ts():
    return datetime.now(timezone.utc).timestamp()
