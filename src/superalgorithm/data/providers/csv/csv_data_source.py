import asyncio
from typing import List
from superalgorithm.data.data_provider import DataSource
from superalgorithm.data.providers.csv import load_historical_data
from superalgorithm.types.data_types import Bar


class CSVDataSource(DataSource):
    def __init__(
        self,
        pair: str,
        timeframe: int,
        aggregations: List[str] = None,
        csv_data_folder: str = ".history",
        since_ts: int = 0,
    ):

        super().__init__(pair, timeframe, aggregations)
        self.since_ts = since_ts
        self.csv_data_folder = csv_data_folder

    async def connect(self):
        self.data = load_historical_data(
            self.source_id, self.timeframe, self.csv_data_folder
        )

        if self.since_ts > 0:
            self.data = [item for item in self.data if item.timestamp > self.since_ts]

    async def read(self):
        while True and len(self.data) > 0:
            ohlcv = self.data.pop(0)
            yield Bar(
                ohlcv.timestamp,
                self.source_id,
                self.timeframe,
                ohlcv,
            ), False
            # send None when we have no more data, signaling the end of the data stream
            if len(self.data) == 0:
                yield None, False

            await asyncio.sleep(0)

    async def disconnect(self):
        pass
