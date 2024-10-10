from typing import Dict, Tuple
from superalgorithm.types.data_types import MarkPrice
from superalgorithm.utils.helpers import get_now_ts


class StatusTracker:
    def __init__(self):
        self._last_seen_price: Dict[Tuple[str, str], MarkPrice] = {}
        self._highest_timestamp = 0

    def update_price(self, pair: str, timestamp: int, mark_price: float):
        """
        Updates the price data for a given pair. If the timestamp is higher than the last seen timestamp for the pair, the price data is updated.
        """
        key = pair
        if (
            key not in self._last_seen_price
            or timestamp > self._last_seen_price[key].timestamp
        ):
            self._last_seen_price[key] = MarkPrice(timestamp, mark_price)
            self._highest_timestamp = max(self._highest_timestamp, timestamp)

    def get_latest_price(self, pair: str) -> MarkPrice:
        """
        Used by paper trading to get the latest price data for a given pair.
        IMPORTANT: Don't use this for live trading.
        """
        key = pair
        if key in self._last_seen_price:
            return self._last_seen_price[key]
        raise ValueError(f"No price data available for pair {pair}")

    def get_highest_timestamp(self) -> int:
        """
        Returns the highest timestamp received so bar by the strategy from all the price data.
        Allows paper trading to work as expected, knowing where we are in time.
        If price data comes out of order, this method, and all paper trading will not work.
        """
        if self._highest_timestamp > 0:
            return self._highest_timestamp
        # when the application starts some modules may need the current timestamp (i.e. logger), so we always return a timestamp
        return get_now_ts()


status_tracker = StatusTracker()


def update_mark_ts(
    pair: str,
    timestamp: int,
    mark_price: float,
    tracker: StatusTracker = status_tracker,
):
    tracker.update_price(pair, timestamp, mark_price)


def get_latest_price(pair: str, tracker: StatusTracker = status_tracker) -> MarkPrice:
    return tracker.get_latest_price(pair)


def get_highest_timestamp(tracker: StatusTracker = status_tracker) -> int:
    return tracker.get_highest_timestamp()
