import pytest
import asyncio
from superalgorithm.exchange.woo_exchange import WOOExchange
from superalgorithm.types.data_types import (
    OrderStatus,
    OrderType,
    PositionType,
)
from superalgorithm.utils.config import config


api_key = config.get("ccxt_config")["api_key"]
api_secret = config.get("ccxt_config")["secret"]
app_id = config.get("ccxt_config")["uid"]
ccxt_config = {
    "apiKey": api_key,
    "secret": api_secret,
    "uid": app_id,
    "hedge_mode": True,
}

symbol = "PNUT/USDT:USDT"


@pytest.fixture(scope="module")
async def setup_exchange():
    exchange = WOOExchange(config=ccxt_config)

    await exchange.start()

    mark_price = await fetch_markprice(exchange, symbol)

    quantity = round(1 / mark_price)  # min 1 USD order required

    await asyncio.sleep(2)  # wait for socket connect

    try:
        yield exchange, mark_price, quantity

    finally:
        await exchange.ccxt_client.close()
        await exchange.stop()


async def fetch_markprice(exchange, symbol):
    """
    Helper method to get the current price for placing test orders.
    """
    kline = await exchange.ccxt_client.fetchOHLCV(symbol)
    mark_price = kline[-1][4]
    return mark_price


async def execute_order_and_wait_close(
    exchange, pair, position_type, quantity, order_type, price
):
    """
    Helper method to place test orders.
    """
    order = None
    order_closed_fut = asyncio.Future()
    if order_type == "open":
        order = await exchange.open(
            pair, position_type, quantity, OrderType.LIMIT, price
        )
        order.on("CLOSED", lambda _: order_closed_fut.set_result(None))
    elif order_type == "close":
        order = await exchange.close(
            pair, position_type, quantity, OrderType.LIMIT, price
        )
        order.on("CLOSED", lambda _: order_closed_fut.set_result(None))

    await order_closed_fut

    return order


@pytest.mark.asyncio(loop_scope="module")
async def test_open_long2(setup_exchange):
    exchange, mark_price, quantity = setup_exchange

    order = await execute_order_and_wait_close(
        exchange, symbol, PositionType.LONG, quantity, "open", mark_price + 0.01
    )

    assert (
        exchange.order_manager.orders[order.client_order_id].order_status
        == OrderStatus.CLOSED
    )
    assert exchange.order_manager.orders[order.client_order_id].filled == quantity

    assert (
        exchange.position_manager.positions[symbol][PositionType.LONG].balance
        == quantity
    )

    # close the position
    order_close = await execute_order_and_wait_close(
        exchange, symbol, PositionType.LONG, quantity, "close", mark_price - 0.01
    )

    assert (
        exchange.order_manager.orders[order_close.client_order_id].order_status
        == OrderStatus.CLOSED
    )
    assert exchange.order_manager.orders[order_close.client_order_id].filled == quantity
    assert exchange.position_manager.positions[symbol][PositionType.LONG].balance == 0


@pytest.mark.asyncio(loop_scope="module")
async def test_cancel_order(setup_exchange):
    exchange, mark_price, quantity = setup_exchange

    order = await exchange.open(
        symbol,
        PositionType.LONG,
        quantity * 2,
        OrderType.LIMIT,
        mark_price - 0.10,
    )

    await exchange.cancel_order(order)
    assert (
        exchange.order_manager.orders[order.client_order_id].order_status
        == OrderStatus.CANCELED
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_cancel_all_orders(setup_exchange):
    exchange, mark_price, quantity = setup_exchange
    order = await exchange.open(
        symbol,
        PositionType.LONG,
        quantity * 2,
        OrderType.LIMIT,
        mark_price - 0.10,
    )
    order2 = await exchange.open(
        symbol,
        PositionType.LONG,
        quantity * 2,
        OrderType.LIMIT,
        mark_price - 0.10,
    )

    response = await exchange.cancel_all_orders()
    assert response, True

    assert (
        exchange.order_manager.orders[order.client_order_id].order_status
        == OrderStatus.CANCELED
    )
    assert (
        exchange.order_manager.orders[order2.client_order_id].order_status
        == OrderStatus.CANCELED
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_rejected_order(setup_exchange):

    exchange, mark_price, quantity = setup_exchange
    order = await exchange.open(
        symbol, PositionType.LONG, 0.0000000001, OrderType.LIMIT, mark_price - 0.1
    )

    assert (
        exchange.order_manager.orders[order.client_order_id].order_status
        == OrderStatus.REJECTED
    )
    assert exchange.order_manager.orders[order.client_order_id].filled == 0


@pytest.mark.asyncio(loop_scope="module")
async def test_get_balances(setup_exchange):
    exchange, mark_price, quantity = setup_exchange
    balances = await exchange.get_balances()
    assert balances.free["USDT"] >= 0


@pytest.mark.asyncio(loop_scope="module")
async def test_hedge_mode(setup_exchange):

    exchange, mark_price, quantity = setup_exchange

    order = await exchange.open(
        symbol,
        PositionType.LONG,
        quantity * 2,
        OrderType.LIMIT,
        mark_price - 0.1,
    )
    order2 = await exchange.open(
        symbol,
        PositionType.SHORT,
        quantity * 2,
        OrderType.LIMIT,
        mark_price + 0.1,
    )

    await exchange.cancel_all_orders(symbol)

    assert order.order_status == OrderStatus.CANCELED
    assert order2.order_status == OrderStatus.CANCELED


@pytest.mark.asyncio(loop_scope="module")
async def test_market_order(setup_exchange):

    exchange, mark_price, quantity = setup_exchange
    order_complete_fut = asyncio.Future()
    order = await exchange.open(
        symbol,
        PositionType.LONG,
        quantity,
        OrderType.MARKET,
    )

    order.on("CLOSED", lambda _: order_complete_fut.set_result(None))

    await order_complete_fut

    order_complete_fut = asyncio.Future()
    order2 = await exchange.open(
        symbol,
        PositionType.SHORT,
        quantity,
        OrderType.MARKET,
    )
    order2.on("CLOSED", lambda _: order_complete_fut.set_result(None))

    await order_complete_fut

    assert order.order_status == OrderStatus.CLOSED
    assert order2.order_status == OrderStatus.CLOSED

    # closing the positions

    order_complete_fut = asyncio.Future()
    order = await exchange.close(
        symbol,
        PositionType.LONG,
        quantity,
        OrderType.MARKET,
    )

    order.on("CLOSED", lambda _: order_complete_fut.set_result(None))

    await order_complete_fut

    order_complete_fut = asyncio.Future()
    order2 = await exchange.close(
        symbol,
        PositionType.SHORT,
        quantity,
        OrderType.MARKET,
    )
    order2.on("CLOSED", lambda _: order_complete_fut.set_result(None))

    await order_complete_fut

    assert exchange.position_manager.positions[symbol][PositionType.LONG].balance == 0
    assert exchange.position_manager.positions[symbol][PositionType.SHORT].balance == 0
