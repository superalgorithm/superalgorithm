import uuid
from datetime import datetime, timedelta


def guid() -> str:
    random_uuid = uuid.uuid4()
    guid = str(random_uuid).replace("-", "")
    return guid


def get_now_ts() -> int:
    return int(datetime.now().timestamp() * 1000)


def get_bucket_size(resolution: str) -> int:
    """
    Get the bucket size in milliseconds for a given resolution. i.e. 1m, 1h, 1d
    """
    resolution_seconds = {"m": 60000, "h": 3600000, "d": 86400000}
    res_value = int(resolution[:-1])
    res_unit = resolution[-1]
    return res_value * resolution_seconds[res_unit]


def get_lookback_timestamp(days: int = None, hours: int = None) -> int:
    """
    Get the timestamp in milliseconds for a given lookback period in days or hours.

    Args:
        days (int, optional): The lookback period in days.
        hours (int, optional): The lookback period in hours.

    Returns:
        int: The timestamp in milliseconds for the lookback period.

    Raises:
        ValueError: If neither days nor hours are provided.
    """
    if days is None and hours is None:
        raise ValueError("At least one of 'days' or 'hours' must be provided")

    current_datetime = datetime.now()

    if days is not None:
        lookback_datetime = current_datetime - timedelta(days=days)
    if hours is not None:
        lookback_datetime = current_datetime - timedelta(hours=hours)
    if days is not None and hours is not None:
        lookback_datetime = current_datetime - timedelta(days=days, hours=hours)

    return int(lookback_datetime.timestamp() * 1000)
