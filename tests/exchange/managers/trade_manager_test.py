import pytest
from unittest.mock import MagicMock
from superalgorithm.exchange.managers.trade_manager import TradeManager
from superalgorithm.types.data_types import Trade, PositionType, TradeType


@pytest.fixture
def mock_exchange():
    """Create a mock exchange with a mock order manager and position manager."""
    exchange = MagicMock()
    exchange.order_manager.get_order_by_server_id.return_value = MagicMock(
        position_type=PositionType.LONG, trade_type=TradeType.OPEN
    )
    exchange.position_manager.add_trade = MagicMock()
    return exchange


@pytest.fixture
def trade_manager(mock_exchange):
    """Instantiate TradeManager with the mock exchange."""
    return TradeManager(exchange=mock_exchange)


@pytest.mark.asyncio
async def test_trade_manager_emits_trade_event(trade_manager):
    """Verify that adding a trade emits the 'trade' event."""

    handler_called = False

    def on_trade(trade):
        nonlocal handler_called
        handler_called = True

    trade_manager.on("trade", on_trade)

    test_trade = Trade(
        trade_id="test_id",
        timestamp=123456,
        pair="BTC/USDT",
        position_type=None,
        trade_type=None,
        price=50000.0,
        quantity=1.0,
        server_order_id="order123",
    )

    await trade_manager.add(test_trade)

    assert handler_called, "Adding a trade did not emit the 'trade' event."
