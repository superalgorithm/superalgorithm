import math
from typing import List
from superalgorithm.types import OHLCV
from superalgorithm.types.data_types import AggregatorResult, Bar
from superalgorithm.utils.event_emitter import EventEmitter
from superalgorithm.utils.helpers import get_bucket_size


class OHLCVAggregator(EventEmitter):
    def __init__(self, source_id: str, timeframe: str):
        """
        Initializes an OHLCVAggregator instance for aggregating OHLCV (Open, High, Low, Close, Volume) data.

        This class provides functionality to aggregate high-frequency OHLCV data into lower-frequency time bars.
        It's particularly useful for converting tick-by-tick or minute-by-minute data into larger timeframes
        such as hourly, daily, or custom periods.

        Parameters:
        source_id (str): A unique identifier for the data source.
        timeframe (str): A string specifying the desired aggregation timeframe.
                          It should be in the format of a number followed by 'm' for minutes,
                          'h' for hours, or 'd' for days.
                          Examples: '5m' for 5 minutes, '2h' for 2 hours, '1d' for 1 day.

        The aggregator maintains an internal state representing the current time bar being aggregated.
        As new data points are processed, it updates this state and returns the current aggregated OHLCV data.

        When a new time bar is encountered, the aggregator automatically resets and starts a new bar.

        Usage:
        - Create an instance with the desired resolution.
        - Call the `update` method for each new OHLCV data point.
        - The `update` method will return the current state of the aggregated data for the ongoing time bar.

        Note:
        - The aggregator aligns time bars to epoch time (timestamp 0).
        - It's designed to handle data points that arrive in chronological order.

        Returns:
            AggregatorResult:
                - current_bar (OHLCV): the currently processed but unfinished bar
                - is_new_bar_started (bool): "True" only when a bar is finished and a new bar has started i.e. 12AM midnight, full hour etc.
                - last_completed_bar (Optional[OHLCV]): the bar that was completed when is_new_bar_started was last sent as "True"


        Timeline: 9 PM to 3 AM daily bar aggregation
        +---------------------------------------------------------------------------+
        | 9 PM       10 PM       11 PM       12 AM       1 AM       2 AM       3 AM |
        |   |           |           |           |           |           |           |
                                                | *nb
        current_bar = July 12th                 last_completed_bar = July 12th
        current_bar = July 13th                 last_completed_bar = July 13th

        * is_new_bar_started is True only for one update when a new bar was started, and then it is False until the next new bar is started.

        NOTE: if your data source is not sending an update at 12AM you will not get a new bar until you receive some data.
        """
        super().__init__()
        self.source_id = source_id
        self.timeframe = timeframe
        self.current_bar = None
        self.previous_bar = None
        self.bucket_size = get_bucket_size(timeframe)
        self.completed_bars: List[OHLCV] = []

    def update(self, data: OHLCV) -> AggregatorResult:
        bucket_start = math.floor(data.timestamp / self.bucket_size) * self.bucket_size
        new_bucket_started = False

        # start a new bucket if the bucket_start is no longer matching the current buckets ts
        if self.current_bar is None or bucket_start != self.current_bar.timestamp:
            self.previous_bar = self.current_bar
            self.current_bar = OHLCV(
                timestamp=bucket_start,
                open=data.open,
                high=data.high,
                low=data.low,
                close=data.close,
                volume=data.volume,
            )
            if (
                self.previous_bar is not None
            ):  # fist time round there is no previous bar
                self.completed_bars.append(self.previous_bar)
                new_bucket_started = True

        else:
            # just update the values
            self.current_bar.high = max(self.current_bar.high, data.high)
            self.current_bar.low = min(self.current_bar.low, data.low)
            self.current_bar.close = data.close
            self.current_bar.volume += data.volume

        return AggregatorResult(
            current_bar=self.current_bar,
            is_new_bar_started=new_bucket_started,
            last_completed_bar=self.previous_bar,
        )

    def __getitem__(self, index: int) -> OHLCV:
        # Allows the class to be subscriptable
        return self.completed_bars[index]
