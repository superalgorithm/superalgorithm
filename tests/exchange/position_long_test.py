import pytest

# from superalgorithm.exchange.position import Position
from superalgorithm.types.data_types import PositionType, Trade, TradeType, Position

import pytest


@pytest.fixture(scope="session")
def position():
    # Create a new position instance once for all tests in this module
    position = Position("BTC/USDT", PositionType.LONG)
    return position


def test_add_first_trade(position):
    position.add_trade(
        Trade(
            trade_id="1",
            timestamp=123,
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.OPEN,
            price=10,
            quantity=100,
            server_order_id="1234",
        )
    )
    assert position.average_open == 10
    assert position.balance == 100


def test_add_close_trade(position):
    position.add_trade(
        Trade(
            trade_id="2",
            timestamp=124,
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.CLOSE,
            price=20,
            quantity=100,
            server_order_id="555",
        )
    )
    assert position.balance == 0
    assert position.average_open == 0
    assert position.get_trade("2").pnl == 1000
    assert position.get_total_pnl == 1000


def test_reopen_position(position):
    position.add_trade(
        Trade(
            trade_id="3",
            timestamp=123,
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.OPEN,
            price=10,
            quantity=100,
            server_order_id="12343",
        )
    )
    assert position.average_open == 10
    assert position.balance == 100


def test_add_additional_open_trade(position):
    position.add_trade(
        Trade(
            trade_id="4",
            timestamp=123,
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.OPEN,
            price=20,
            quantity=100,
            server_order_id="12344",
        )
    )
    assert position.average_open == 15
    assert position.balance == 200


def test_partial_close_trade(position):
    position.add_trade(
        Trade(
            trade_id="5",
            timestamp=123,
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.CLOSE,
            price=15,
            quantity=50,
            server_order_id="12344",
        )
    )
    # we sell at break even so trade pnl is 0 and total pnl remains unchanged
    assert position.balance == 150
    assert position.average_open == 15
    assert position.get_trade("5").pnl == 0
    assert position.get_total_pnl == 1000


def test_full_close_trade(position):
    position.add_trade(
        Trade(
            trade_id="6",
            timestamp=123,
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.CLOSE,
            price=10,
            quantity=50,
            server_order_id="12345",
        )
    )
    assert position.balance == 100
    assert position.average_open == 15
    assert position.get_trade("6").pnl == -250
    assert position.get_total_pnl == 1000 - 250
