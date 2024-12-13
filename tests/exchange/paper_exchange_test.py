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
