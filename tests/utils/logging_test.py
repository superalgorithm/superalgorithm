import asyncio
import pytest
from superalgorithm.exchange.paper_exchange import PaperExchange
from superalgorithm.exchange.status_tracker import update_mark_ts
from superalgorithm.utils.logging import (
    set_chart_schema,
    annotate,
    log_exception,
    monitor,
    chart,
    log_trade,
    log_order,
)
from superalgorithm.types.data_types import (
    OHLCV,
    ChartPointDataType,
    ChartSchema,
    Order,
    OrderStatus,
    OrderType,
    PositionType,
    Trade,
    TradeType,
)
from superalgorithm.utils.helpers import get_now_ts
from superalgorithm.utils.logging import strategy_monitor

# we reset the strategy_monitor as the other tests create a lot of data and we don't want that reported here.
strategy_monitor.clear()


def test_monitor_only_logs_one():
    monitor("my_variable", 1)
    monitor("my_variable", 2)
    monitor("other_variable", 4)

    assert strategy_monitor._monitoring["my_variable"].value == 2
    assert strategy_monitor._monitoring["other_variable"].value == 4


def test_annotate():
    annotate(1, "here we buy")
    assert len(strategy_monitor._annotations) == 1


def test_chart_logs_integers():
    set_chart_schema(
        [
            ChartSchema("ema", ChartPointDataType.FLOAT, "scatter", "red"),
            ChartSchema("btc", ChartPointDataType.OHLCV, "candlestick", "red"),
            ChartSchema("sma", ChartPointDataType.FLOAT, "scatter", "red"),
        ]
    )
    chart("ema", 1, 1727343184064)
    chart("ema", 2, 1727343184065)
    chart("ema", 3, 1727343184066)
    chart("btc", OHLCV(1727343184066, 1, 2, 3, 4, 5), 1727343184066)
    chart("sma", 4, 1727343184064)
    chart("sma", 5, 1727343184065)
    chart("sma", 6, 1727343184066)

    chart_json = strategy_monitor.convert_chart_data(strategy_monitor._chart_points)
    print(chart_json["data"])
    assert len(chart_json["data"].keys()) == 3, "chart_json should contain 3 keys"
    assert chart_json["data"][1727343184066] == [
        3,
        1,
        2,
        3,
        4,
        5,
        6,
    ], "chart_json btc value was set correctly"

    assert len(strategy_monitor._chart_points) == 7


def test_exception():
    try:
        raise ValueError("I am an error")
    except Exception as e:
        log_exception(e)


def test_trade():

    trade = Trade(
        trade_id="ok",
        timestamp=get_now_ts(),
        pair="BTC/USDT",
        position_type=PositionType.LONG,
        trade_type=TradeType.OPEN,
        price=12,
        quantity=12,
        server_order_id="23",
        pnl=10,
    )
    log_trade(trade=trade)

    trade = Trade(
        trade_id="ok2",
        timestamp=get_now_ts(),
        pair="BTC/USDT",
        position_type=PositionType.LONG,
        trade_type=TradeType.OPEN,
        price=12,
        quantity=12,
        server_order_id="23",
        pnl=10,
    )
    log_trade(trade=trade)


def test_order():
    order = Order(
        pair="FTM/USDT",
        position_type=PositionType.LONG,
        trade_type=TradeType.CLOSE,
        quantity=12,
        price=10,
        order_type=OrderType.LIMIT,
        order_status=OrderStatus.CANCELED,
        client_order_id="12",
        server_order_id="e554",
        timestamp=get_now_ts(),
        filled=1,
    )

    log_order(order)


async def create_dummy_order(exchange):
    order_closed_fut = asyncio.Future()
    update_mark_ts("BTC/USDT", get_now_ts(), 12)
    order1 = await exchange.open("BTC/USDT", PositionType.LONG, 1, OrderType.LIMIT, 12)
    order1.on("CLOSED", lambda _: order_closed_fut.set_result(None))
    await order_closed_fut


@pytest.mark.asyncio
async def test_scheduled_run():

    upload_complete_fut = asyncio.Future()
    exchange = PaperExchange()
    exchange.start()

    await create_dummy_order(exchange)
    await create_dummy_order(exchange)
    await create_dummy_order(exchange)

    strategy_monitor.set_upload_interval(2)
    strategy_monitor.on("upload_complete", lambda: upload_complete_fut.set_result(None))

    strategy_monitor.start()
    # asyncio.gather(strategy_monitor.start(), return_exceptions=True)

    await upload_complete_fut

    strategy_monitor.stop()
    exchange.stop()
