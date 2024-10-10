from collections import defaultdict
from typing import DefaultDict, List
from superalgorithm.types.data_types import OHLCV


class DataStore:
    def __init__(self) -> None:
        """
        Creates a nested defaultdict where each source and timeframe combination
        maps to a list of OHLCV data points: store[source][timeframe] -> List[OHLCV]
        """
        self.store: DefaultDict[str, DefaultDict[str, list]] = defaultdict(
            lambda: defaultdict(list)
        )

    def append(self, source_id: str, ohlcv: OHLCV, timeframe: str = "raw"):
        """
        Appends an OHLCV data point to the store for a specified source and timeframe.
        """
        self.store[source_id][timeframe].append(ohlcv)

    def last(self, source_id: str, timeframe: str) -> OHLCV:
        """
        Retrieves the most recent OHLCV data point for a specified source and timeframe.
        """
        if len(self.store[source_id][timeframe]) > 0:
            return self.store[source_id][timeframe][-1]

        else:
            return None

    def update(self, source_id: str, timeframe: str, ohlcv: OHLCV):
        """
        Updates the most recent OHLCV data point for a specified source and timeframe.
        """
        if len(self.store[source_id][timeframe]) > 0:
            self.store[source_id][timeframe][-1] = ohlcv

    def list(self, source_id: str, timeframe: str) -> List[OHLCV]:
        """
        Retrieves all OHLCV data points for a specified source and timeframe.
        """
        return self.store[source_id][timeframe]
