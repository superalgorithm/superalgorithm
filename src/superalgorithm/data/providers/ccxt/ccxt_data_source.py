from typing import List
from superalgorithm.data.providers.ccxt.ccxt_helper import fetch_ohlcv
from superalgorithm.data.data_provider import DataSource
from superalgorithm.types import OHLCV
import ccxt.pro as ccxtpro
from superalgorithm.types.data_types import Bar
from superalgorithm.utils.logging import log_exception, log_message


class CCXTDataSource(DataSource):
    def __init__(
        self,
        pair: str,
        timeframe: int,
        aggregations: List[str] = None,
        exchange_id: str = "binance",
        preload_from_ts: int = 0,
    ):
        """
        Initializes the CCXTDataSource with the given parameters.

        Args:
            pair (str): The trading pair to fetch data for (e.g., "BTC/USDT").
            timeframe (int): The timeframe of the data (e.g., 5m, 1h).
            aggregations (List[str], optional): List of timeframes to aggregate data into.
            exchange_id (str, optional): The ID of the exchange to connect to (default is "binance").
            preload_from_ts (int, optional): Timestamp from which to start preloading historical data example int(datetime.now().timestamp() * 1000).
        """

        self.is_connected = False
        self.exchange_id = exchange_id
        self.exchange = getattr(ccxtpro, exchange_id)()
        self.preload_from_ts = preload_from_ts
        super().__init__(pair, timeframe, aggregations)

    async def connect(self):
        """
        Establishes a connection to the exchange. This method should be called before starting to read data.
        """
        self.is_connected = True
        log_message("CCXTDataSource connected")

    async def read(self):
        """
        This method yields Bar objects containing either historical or real-time data.

        - If preload_from_ts is set, it fetches and yields historical data first.
        - Then, it starts reading real-time data from the exchange.

        Yields:
            Bar: An update containing timestamped OHLCV data and any calculated aggregates.
            Bool: A flag indicating whether the data is live or historical.
        """

        # fetch and playback historical data
        if self.preload_from_ts > 0:
            ohlcv_list = fetch_ohlcv(
                symbol=self.source_id,
                timeframe=self.timeframe,
                start_ts=self.preload_from_ts,
                exchange_id=self.exchange_id,
            )
            for ohlcv in ohlcv_list:
                yield Bar(
                    ohlcv.timestamp,
                    self.source_id,
                    self.timeframe,
                    ohlcv,
                ), False

        # start real time data reading
        while self.is_connected:
            try:

                candles = await self.exchange.watch_ohlcv(
                    self.source_id, self.timeframe
                )

                for candle in candles:

                    yield Bar(
                        candle[0],
                        self.source_id,
                        self.timeframe,
                        OHLCV(
                            timestamp=candle[0],
                            open=candle[1],
                            high=candle[2],
                            low=candle[3],
                            close=candle[4],
                            volume=candle[5],
                        ),
                    ), True

            except Exception as e:
                log_exception(e, "Error receiving data")

    async def disconnect(self):
        """
        Disconnects from the exchange and performs any necessary cleanup. This method should be called to stop data streaming.
        """
        await self.exchange.close()
        self.is_connected = False
        log_message("CCXTDataSource disconnected")
