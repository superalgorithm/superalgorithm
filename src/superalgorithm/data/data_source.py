from typing import List, Dict, Tuple
from abc import ABC, abstractmethod
from superalgorithm.data.ohlcv_aggregator import OHLCVAggregator
from superalgorithm.types.data_types import OHLCV, AggregatorResult, Bar


class DataSource(ABC):

    def __init__(
        self,
        source_id: str,
        timeframe: str,
        aggregations: List[str] = None,
    ) -> None:
        """
        Initializes a new instance of the data source object.

        Args:
            source_id (str):
                A unique identifier for the data source, such as a trading pair ("BTC/USDT").
                This ID is used to distinguish between different data sources in the system.

            timeframe (str):
                The timeframe of the data you want to read. It defines the frequency at which data points are recorded, such as "1m" (1 minute), "15m" (15 minutes), "1h" (1 hour), "1d" (1 day), etc.

            aggregations (List[str], optional):
                A list of higher timeframes for which aggregated data should be calculated. Each aggregation should be a valid timeframe that is longer than the primary timeframe (e.g., if the primary timeframe is "1m", valid aggregations might include "15m", "1h", "1d"). If no aggregations are specified, this defaults to None.
        """

        self.source_id = source_id
        self.timeframe = timeframe

        if aggregations and timeframe in aggregations:
            raise ValueError(
                f"aggregations {aggregations} for {source_id} must not include the original timeframe {timeframe}"
            )

        self.aggregations: Dict[str, OHLCVAggregator] = (
            {
                timeframe: OHLCVAggregator(source_id=source_id, timeframe=timeframe)
                for timeframe in aggregations
            }
            if aggregations
            else {}
        )
        # NOTE: Live exchange datasources may send multiple updates per bar ("candle") or even ticks. Hence, the original timeframe will also be "aggregated", this way we can process and access all timeframes in the same way i.e. listen to their on(timeframe) events or get(source, timeframe) etc..
        self.aggregations[self.timeframe] = OHLCVAggregator(
            self.source_id, self.timeframe
        )

    @abstractmethod
    async def connect(self):
        """
        Establishes a connection to the data source.

        The subclass class must implement this method and use it to initiate the connection process,
        which may involve loading data, connecting to a socket, or any other necessary
        setup procedures required to interact with the data source.
        """
        pass

    @abstractmethod
    async def read(self) -> Tuple[Bar, bool]:
        """
        Continuously reads data from the data source and yields updates.

        The implementing class is required to define this method to continuously fetch
        data from the source and yield `Bar` objects as they become available.
        This method acts as an asynchronous generator, providing historical, real-time or batched
        updates from the data source.

        Yields:
            - Bar: A `Bar` object representing the latest data update.
            - bool: A flag indicating whether the data is live or historical.
        """
        pass

    @abstractmethod
    async def disconnect(self):
        """
        Shuts down the connection and performs cleanup.

        The subclass class should implement this method to handle the disconnection process
        from the data source, ensuring that any open connections are properly closed and
        resources are released.
        """
        pass

    def update_aggregates(
        self, ohlcv: OHLCV
    ) -> List[Tuple[str, str, AggregatorResult]]:
        """
        Updates aggregates for the data source.

        Returns a list of tuples containing the "source ID", "timeframe", and the "result of the aggregation".

        If your datasource does not send OHLCV data but only ticks, you can use this method to aggregate the data into OHLCV bars.
        Simply set the same value for o,h,l and c.
        """

        result = []
        if self.aggregations:
            for timeframe, aggregator in self.aggregations.items():
                agg_result = aggregator.update(ohlcv)

                result.append([self.source_id, timeframe, agg_result])

        return result
