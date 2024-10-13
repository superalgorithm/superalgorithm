from abc import abstractmethod
import asyncio
from typing import List
import warnings
from superalgorithm.data.data_provider import DataProvider
from superalgorithm.data.data_source import DataSource
from superalgorithm.data.data_store import DataStore
from superalgorithm.exchange.base_exchange import BaseExchange
from superalgorithm.types.data_types import (
    OHLCV,
    Balances,
    Bar,
    ExchangeType,
    ExecutionMode,
    Order,
    OrderType,
    Position,
    PositionType,
)
from superalgorithm.utils.event_emitter import EventEmitter
from superalgorithm.utils.logging import log_message, strategy_monitor
from superalgorithm.exchange.status_tracker import (
    update_mark_ts,
)


class BaseStrategy(EventEmitter):
    def __init__(self, data_sources: list[DataSource], exchange: BaseExchange):
        super().__init__()

        self.data_provider = DataProvider()
        self.exchange = exchange
        self.data_store = DataStore()
        self._async_tasks = []

        self.mode = (
            ExecutionMode.PRELOAD
            if exchange.exchange_type == ExchangeType.LIVE
            else ExecutionMode.PAPER
        )
        log_message(f"Mode set to {self.mode}")

        [self.data_provider.add_data_source(ds) for ds in data_sources]

    @abstractmethod
    def init(self):
        """
        Configure indicators and make any one time setup configurations.
        """
        pass

    @abstractmethod
    async def on_tick(self, bar: Bar):
        """
        Implement this method to update your indicators, perform computations or trigger trade logic.

        Note: on_tick triggers individually for every configured DataSource.
        During a backtest this may be one update per bar as per your test data (i.e. you are loading hourly bars),
        but during live trading you may get many updates per bar.

        Alternatively, You can use the on(timeframe) event to listen to completed bars instead.

        Inside "on_tick" the helper method "is_new_bar()" can be used to find out if a new bar has started:

        if new_bar("1m"):
            print("New minute has started")
            bar = update.get("1m")
            #update minute indicators
        if new_bar("15m"):
            print("New 15 minute bar has started")
            #update 15 minute indicators and trigger trade logic

        ## How to access data:

        Access the tick data using the bar object:
        ohlcv = bar.ohlcv

        Access the bars using the data store:

        bars = self.data("BTC/USDT", "1m")
        bar = self.get("BTC/USDT", "1m")

        Executing trades:

        Call trade logic from on_tick or on(timeframe) events. Remember that on_tick triggers for every update,
        so you may want to use the on(timeframe) event to trigger trades or check is_new_bar().

        self.trade_logic()
        """
        pass

    @abstractmethod
    async def trade_logic(self):
        """
        Write conditional logic to execute trades, stops etc.
        """

        pass

    async def _process_bar(self, bar):
        """
        Compute the aggregates and for each aggregation update the data store.
        """
        result = self.data_provider.data_sources[bar.source_id].update_aggregates(
            bar.ohlcv
        )
        for source_id, timeframe, agg_result in result:
            await self._update_data_store(source_id, timeframe, agg_result)
        update_mark_ts(bar.source_id, bar.ohlcv.timestamp, bar.ohlcv.close)
        await self.on_tick(bar)

    async def _update_data_store(self, source_id, timeframe, agg_result):
        """
        Updates or appends the data store with the new bar data and dispatches the on(timeframe) events for all new bars.

        Important to note that nothing happens for the first bar of any aggregation.
        Once a new bar is detected we ensure the datastore contains the last completed bar, dispatch the event and store the new bar.
        The users of the strategy that operate on the on(timeframe) event will receive the last completed bar from both the event hander and the datastore.

        +---------------------------------------------------------------------------+
        08 PM           09 PM           10 PM           11 PM            12 AM
        | bar 1         | bar 2         | bar 3         | bar 4          | ...
        start ->        + append bar 1
        building        + dispatch bar 1
        bar 1           + user consumes bar 1
                        + store.last contains bar 2 updates until new bar is created at 10 PM

        """
        if agg_result.is_new_bar_started:
            if len(self.data_store.list(source_id, timeframe)) == 0:
                self.data_store.append(
                    source_id=source_id,
                    timeframe=timeframe,
                    ohlcv=agg_result.last_completed_bar,
                )
            await self.dispatch_and_await(
                timeframe,
                Bar(
                    agg_result.last_completed_bar.timestamp,
                    source_id,
                    timeframe,
                    agg_result.last_completed_bar,
                ),
            )
            self.data_store.append(
                source_id=source_id, timeframe=timeframe, ohlcv=agg_result.current_bar
            )
        else:
            self.data_store.update(
                source_id=source_id, timeframe=timeframe, ohlcv=agg_result.current_bar
            )

    async def _process_paper_trades(self):
        """
        During paper trading the paper exchange will create _simulate_trade_execution calls.
        We will wait until all of them are processed. This works because the paper exchange at this time executes any order instantly.
        TODO: if we implement limit order for paper trading we have to update this logic.
        """
        tasks_to_process = list(self.exchange.trade_tasks)
        self.exchange.trade_tasks.clear()
        await asyncio.gather(*tasks_to_process, return_exceptions=True)

    def _switch_to_live_mode(self):
        self.mode = ExecutionMode.LIVE
        log_message(f"Mode set to {self.mode}")

    async def start(self):

        self.init()

        # TODO: most likely we can start exchanges by default...
        self._async_tasks.extend(self.exchange.start())

        # no monitoring for backtests
        if not self.mode == ExecutionMode.PAPER:
            self._async_tasks.append(strategy_monitor.start())

        async for bar, is_live in self.data_provider.stream_data():

            if self.mode == ExecutionMode.PAPER and bar is None:
                self.dispatch("backtest_done", self)
                break  # break the event loop

            if self.mode == ExecutionMode.PRELOAD and is_live:
                self._switch_to_live_mode()

            await self._process_bar(bar)

            # during paper trading, we wait for trades to fill orders
            if self.mode == ExecutionMode.PAPER:
                await self._process_paper_trades()
            else:
                await asyncio.sleep(0)

    async def stop(self):
        self.exchange.stop()
        current_task = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current_task]
        await asyncio.gather(*tasks, return_exceptions=True)
        for task in tasks:
            task.cancel()

    def data(self, source_id: str, timeframe: str) -> List[OHLCV]:
        """
        Returns the list of ohlcv values for a given source and timeframe.
        """
        return self.data_store.list(source_id, timeframe)

    def get(self, source_id: str, timeframe: str) -> OHLCV:
        """
        Returns the last ohlcv value for the given source and timeframe, including any in progress bar aggregates.
        """
        return self.data_store.last(source_id, timeframe)

    async def cancel_order(self, order: Order) -> bool:
        return await self.exchange.cancel_order(order)

    async def cancel_all_orders(self, pair: str = None) -> bool:
        return await self.exchange.cancel_all_orders(pair)

    async def get_balances(self) -> Balances:
        return await self.exchange.get_balances()

    def get_position(self, pair: str, position_type: PositionType) -> Position:
        return self.exchange.get_or_create_position(pair, position_type)

    async def open(
        self,
        pair: str,
        position_type: PositionType,
        quantity: float,
        order_type: OrderType,
        price: float = 0,
    ):
        if self.mode == ExecutionMode.PRELOAD:
            warnings.warn("Skipping open order in PRELOAD mode.")
            return None

        await self.exchange.open(pair, position_type, quantity, order_type, price)

    async def close(
        self,
        pair: str,
        position_type: PositionType,
        quantity: float,
        order_type: OrderType,
        price: float = 0,
    ):
        if self.mode == ExecutionMode.PRELOAD:
            warnings.warn("Skipping close order in PRELOAD mode.")
            return None

        await self.exchange.close(pair, position_type, quantity, order_type, price)
