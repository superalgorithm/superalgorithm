import pytest
import asyncio
from datetime import datetime
from superalgorithm.exchange.base_exchange import InsufficientFundsError
from superalgorithm.exchange.paper_exchange import PaperExchange
from superalgorithm.exchange.status_tracker import update_mark_ts
from superalgorithm.types.data_types import OrderType, PositionType
from superalgorithm.utils.logging import strategy_monitor

"""
Orders in the paper exchange are submitted and closed via an async trade, just like in the real world.
This means if you place and order, it's not immediately filled and CLOSED.

Mark Price: the paper exchange requires a mark price and the latest timestamp during a backtest. 
This is handled automatically by the BaseStrategy while processing data, but for the tests below we have to set this manually.
"""


@pytest.fixture
async def setup_exchange():
    exchange = PaperExchange(initial_cash=10000)
    await exchange.start()

    await asyncio.sleep(0.1)

    try:
        yield exchange
    finally:
        await exchange.stop()


async def test_open_long_paper(setup_exchange):
    exchange = setup_exchange
    # the paper exchange needs a mark price to know what time it is
    update_mark_ts("BTC/USD", int(datetime.now().timestamp()), 5000)

    order_closed_fut = asyncio.Future()

    (
        await exchange.open(
            pair="BTC/USD",
            position_type=PositionType.LONG,
            quantity=1.0,
            order_type=OrderType.LIMIT,
            price=5000.0,
        )
    ).on("CLOSED", lambda _: order_closed_fut.set_result(None))

    await order_closed_fut

    assert exchange.cash == 5000

    update_mark_ts("BTC/USD", int(datetime.now().timestamp()), 4000)

    order_closed_fut = asyncio.Future()

    (
        await exchange.close(
            pair="BTC/USD",
            position_type=PositionType.LONG,
            quantity=1.0,
            order_type=OrderType.LIMIT,
            price=4000.0,
        )
    ).on("CLOSED", lambda _: order_closed_fut.set_result(None))

    await order_closed_fut

    assert exchange.cash == 9000


async def test_long_out_of_cash(setup_exchange):
    exchange = setup_exchange
    # the paper exchange needs some mark price to know what time it is
    update_mark_ts("BTC/USD", int(datetime.now().timestamp()), 5000)

    with pytest.raises(InsufficientFundsError):
        await exchange.open(
            pair="BTC/USD",
            position_type=PositionType.LONG,
            quantity=3.0,
            order_type=OrderType.LIMIT,
            price=5000.0,
        )


@pytest.mark.asyncio
async def test_close_long_insufficient_balance(setup_exchange):
    exchange = setup_exchange
    # the paper exchange needs some mark price to know what time it is
    update_mark_ts("BTC/USD", int(datetime.now().timestamp()), 5000)

    order_closed_fut = asyncio.Future()

    (
        await exchange.open(
            pair="BTC/USD",
            position_type=PositionType.LONG,
            quantity=2.0,
            order_type=OrderType.LIMIT,
            price=5000.0,
        )
    ).on("CLOSED", lambda _: order_closed_fut.set_result(None))

    await order_closed_fut

    with pytest.raises(InsufficientFundsError):
        await exchange.close(
            pair="BTC/USD",
            position_type=PositionType.LONG,
            quantity=6.0,
            order_type=OrderType.LIMIT,
            price=5000.0,
        )


@pytest.mark.asyncio
async def test_open_short(setup_exchange):
    exchange = setup_exchange

    # the paper exchange needs some mark price to know what time it is
    update_mark_ts("BTC/USD", int(datetime.now().timestamp()), 5000)

    order_closed_fut = asyncio.Future()

    (
        await exchange.open(
            pair="BTC/USD",
            position_type=PositionType.SHORT,
            quantity=1.0,
            order_type=OrderType.LIMIT,
            price=5000.0,
        )
    ).on("CLOSED", lambda _: order_closed_fut.set_result(None))

    await order_closed_fut

    update_mark_ts("BTC/USD", int(datetime.now().timestamp()), 4000)

    order_closed_fut = asyncio.Future()

    (
        await exchange.close(
            pair="BTC/USD",
            position_type=PositionType.SHORT,
            quantity=1.0,
            order_type=OrderType.LIMIT,
            price=4000.0,
        )
    ).on("CLOSED", lambda _: order_closed_fut.set_result(None))

    await order_closed_fut

    assert exchange.cash == 11000


@pytest.mark.asyncio
async def test_short_out_of_cash(setup_exchange):
    exchange = setup_exchange
    # the paper exchange needs some mark price to know what time it is
    update_mark_ts("BTC/USD", int(datetime.now().timestamp()), 5000)

    with pytest.raises(InsufficientFundsError):
        await exchange.open(
            pair="BTC/USD",
            position_type=PositionType.SHORT,
            quantity=3.0,
            order_type=OrderType.LIMIT,
            price=5000.0,
        )


@pytest.mark.asyncio
async def test_close_short_insufficient_balance(setup_exchange):
    exchange = setup_exchange
    # the paper exchange needs some mark price to know what time it is
    update_mark_ts("BTC/USD", int(datetime.now().timestamp()), 5000)

    order_closed_fut = asyncio.Future()

    (
        await exchange.open(
            pair="BTC/USD",
            position_type=PositionType.SHORT,
            quantity=2.0,
            order_type=OrderType.LIMIT,
            price=5000.0,
        )
    ).on("CLOSED", lambda _: order_closed_fut.set_result(None))

    await order_closed_fut

    with pytest.raises(InsufficientFundsError):
        await exchange.close(
            pair="BTC/USD",
            position_type=PositionType.SHORT,
            quantity=3.0,
            order_type=OrderType.LIMIT,
            price=5000.0,
        )


@pytest.mark.asyncio
async def test_stress_test(setup_exchange):
    """
    Expected result is to count 1000 closed orders during a 1000 step loop.
    Order filling is async through the simulation of a trade in the paper exchange (just like a live exchange), but we want the paper exchange to close trades on each bar and not clog up.
    One option is to await asyncio.sleep(0.001) and give "enough" time for trades to confirm orders, before the next tick, but its not clear what is "enough".

    The general behavior is:
    while loop without await asyncio.sleep(0) will not give the event loop any chance to schedule tasks.
    while loop with await asyncio.sleep(0) will schedule tasks, but the tasks may run once the while loop is finished.
    await loop asyncio.sleep(0.001) will give enough room for short tasks to complete

    Instead, we use await asyncio.gather(*trades_to_process) which takes all scheduled trades from the paper exchange and waits for them to conclude. (Also faster than asyncio.sleep(0.001))

    BaseStrategy mirrors the below behavior for paper trading.
    """
    exchange: PaperExchange = setup_exchange

    order_count = 1000
    has_order = False
    orders_filled = 0

    sem = asyncio.Semaphore(10)

    def close_handler(order):
        nonlocal has_order, orders_filled
        has_order = False
        orders_filled += 1

    while order_count != 0:

        update_mark_ts("BTC/USD", int(datetime.now().timestamp() * 1000), 1)

        if not has_order:

            order = await exchange.open(
                pair="BTC/USD",
                position_type=PositionType.SHORT,
                quantity=1.0,
                order_type=OrderType.LIMIT,
                price=1.0,
            )

            order.on("CLOSED", close_handler)  # order_closed_fut.set_result(None)
            has_order = True

        trades_to_process = list(exchange.trade_tasks)
        exchange.trade_tasks.clear()
        await asyncio.gather(*trades_to_process)

        order_count -= 1

    assert orders_filled == 1000

    strategy_monitor.clear()
    await strategy_monitor.stop()

    await exchange.stop()
