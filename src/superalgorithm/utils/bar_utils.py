from collections import defaultdict
from superalgorithm.utils.helpers import get_bucket_size
from superalgorithm.exchange.status_tracker import get_highest_timestamp

timeframe_start = defaultdict(lambda: 0)


def is_new_bar(timeframe: str) -> bool:
    """
    Check if a new bar has started for the given timeframe based on the current time.
    """
    timeframe_ms = get_bucket_size(timeframe)

    current_ts = get_highest_timestamp()

    current_interval = current_ts // timeframe_ms

    # on the first call to is_new_bar intervals[timeframe] is 0, hence we set it to current_interval to start the tracking process.
    if timeframe_start[timeframe] == 0:
        timeframe_start[timeframe] = current_interval

    if current_interval > timeframe_start[timeframe]:
        timeframe_start[timeframe] = current_interval
        return True

    return False
