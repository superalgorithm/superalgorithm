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
    """Verify that adding trades emits the 'trade' event."""

    on_trade_mock = MagicMock()

    trade_manager.on("trade", on_trade_mock)

    test_trade_1 = Trade(
        trade_id="test_id_1",
        timestamp=123456,
        pair="BTC/USDT",
        position_type=None,
        trade_type=None,
        price=50000.0,
        quantity=1.0,
        server_order_id="order123",
    )

    test_trade_2 = Trade(
        trade_id="test_id_2",
        timestamp=123457,
        pair="BTC/USDT",
        position_type=None,
        trade_type=None,
        price=50001.0,
        quantity=2.0,
        server_order_id="order124",
    )

    await trade_manager.add(test_trade_1)
    await trade_manager.add(test_trade_2)

    assert (
        on_trade_mock.call_count == 2
    ), "The 'trade' event handler was not called twice."
