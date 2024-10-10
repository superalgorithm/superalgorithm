from unittest.mock import Mock
import pytest
from runmachine.data.providers.csv import CSVDataSource
from runmachine.exchange.paper_exchange import PaperExchange
from runmachine.strategy.base_strategy import BaseStrategy
from runmachine.types.data_types import (
    Bar,
    ChartPointDataType,
    ChartSchema,
    OrderType,
    PositionType,
)
from runmachine.utils.api_client import api_call
from runmachine.utils.bar_utils import is_new_bar
from runmachine.utils.logging import chart, set_chart_schema, strategy_monitor


class TestStrategy(BaseStrategy):

    def init(self):
        self.tick_called = False
        self.on("5m", self.on_5m)
        self.on("1h", self.on_1h)

        set_chart_schema(
            [ChartSchema("BTC/USDT", ChartPointDataType.OHLCV, "line", "red")]
        )

    async def on_5m(self, bar: Bar):
        self.tick_called = True
        assert isinstance(bar, Bar)
        assert bar.close == self.get("BTC/USDT", "5m").close
        chart(
            "BTC/USDT", bar.ohlcv
        )  # TODO: when we pass just bar, which is wrong, it breaks internally but no error is given we can fix?
        await self.trade_logic()

    async def on_1h(self, bar: Bar):
        self.tick_called = True
        assert isinstance(bar, Bar)
        assert bar.close == self.get("BTC/USDT", "1h").close

    async def on_tick(self, bar: Bar):
        self.tick_called = True
        assert isinstance(bar, Bar)

        if is_new_bar("1h"):
            self.new_bar_called = True
            assert bar.timestamp % 3600000 == 0  # Full hour
            assert bar.close == self.get("BTC/USDT", "1h").close
            await self.trade_logic()

    async def trade_logic(self):
        close = self.get("BTC/USDT", "5m").close
        if close == 64120.39 or close == 60313.31:
            await self.exchange.open(
                "BTC/USDT", PositionType.LONG, 1, OrderType.LIMIT, close
            )

        if close == 61427.72 or close == 61371.27:
            await self.exchange.close(
                "BTC/USDT", PositionType.LONG, 1, OrderType.LIMIT, close
            )
        pass


@pytest.mark.asyncio
async def test_strategy():
    print("start")

    complete_handler_mock_5m = Mock()
    complete_handler_mock_1h = Mock()
    backtest_done_handler_test = Mock()

    async def backtest_done_handler(strategy):
        api_call("v1-backtest", json=strategy_monitor.serialize())
        await strategy.stop()

    source = CSVDataSource("BTC/USDT", "5m", ["1h"])

    num_records = (
        12279  # we know how many records are in the test data, but this is not ideal
    )

    exchange = PaperExchange(initial_cash=100000)
    strategy = TestStrategy(data_sources=[source], exchange=exchange)

    strategy.on("backtest_done", backtest_done_handler)
    strategy.on("backtest_done", backtest_done_handler_test)
    strategy.on("5m", complete_handler_mock_5m)
    strategy.on("1h", complete_handler_mock_1h)
    await strategy.start()

    # verify that the 1h handler was called
    assert complete_handler_mock_1h.call_count > 0

    args, kwargs = complete_handler_mock_1h.call_args
    bar_1h = args[0]
    assert bar_1h.timeframe == "1h"
    assert bar_1h.timestamp % 3600000 == 0  # Full hour
    assert bar_1h.timestamp != 0
    assert bar_1h.source_id == "BTC/USDT"

    assert num_records // 12 == complete_handler_mock_1h.call_count

    # verify that the 5m handler was called
    assert complete_handler_mock_5m.call_count > 0
    args, kwargs = complete_handler_mock_5m.call_args
    bar_5m = args[0]
    assert bar_5m.timeframe == "5m"
    assert bar_5m.timestamp % 30000 == 0  # Full 5m
    assert bar_5m.timestamp != 0
    assert bar_5m.source_id == "BTC/USDT"

    assert num_records - 1 == complete_handler_mock_5m.call_count

    assert len(strategy.data("BTC/USDT", "5m")) == num_records

    assert backtest_done_handler_test.call_count == 1
