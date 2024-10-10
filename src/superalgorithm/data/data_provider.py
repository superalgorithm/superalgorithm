import asyncio
from typing import Dict
from superalgorithm.data.data_source import DataSource


class DataProvider:

    def __init__(self):
        """
        The DataProvider class manages the flow of data between one or more data sources and the strategy that consumes the data.

        The `stream_data` method continuously streams data to the consuming strategy, while reading updates from each DataSource to the async queue.
        """

        self.data_sources: Dict[str, DataSource] = {}
        self.data_sources_is_live: Dict[str, bool] = {}

    def add_data_source(self, data_source: DataSource):
        if data_source.source_id in self.data_sources:
            raise ValueError("Error, same source_id already exists.")
        self.data_sources[data_source.source_id] = data_source
        self.data_sources_is_live[data_source.source_id] = False

    async def producer(self, queue, func):
        async for bar, is_live in func():
            await queue.put([bar, is_live])

    async def stream_data(self):

        tasks = [source.connect() for source in self.data_sources.values()]
        await asyncio.gather(*tasks, return_exceptions=True)

        queue = asyncio.Queue()

        producers = [
            asyncio.create_task(self.producer(queue, source.read))
            for source in self.data_sources.values()
        ]

        try:
            while True:
                bar, is_live = await queue.get()
                if (
                    bar is not None
                ):  # bar is "None" when a backtest data source has no more data to send
                    self.data_sources_is_live[bar.source_id] = is_live
                yield bar, all(self.data_sources_is_live.values())

        finally:
            for task in producers:
                task.cancel()
            await asyncio.gather(*producers, return_exceptions=True)
