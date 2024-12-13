from abc import abstractmethod
from typing import Optional
from superalgorithm.exchange.status_tracker import get_highest_timestamp
from superalgorithm.types.data_types import (
    ExchangeType,
    Order,
    OrderType,
    OrderStatus,
    TradeType,
    PositionType,
    Balances,
)
from superalgorithm.utils.async_task_manager import AsyncTaskManager
from superalgorithm.utils.event_emitter import EventEmitter
from superalgorithm.utils.logging import (
    log_message,
)


class InsufficientFundsError(Exception):
    """Exception raised when there are insufficient funds to open a position."""

    pass


class BaseExchange(EventEmitter, AsyncTaskManager):

    def __init__(self):
        EventEmitter.__init__(self)
        AsyncTaskManager.__init__(self)

        from superalgorithm.exchange.managers.order_manager import OrderManager
        from superalgorithm.exchange.managers.position_manager import PositionManager
        from superalgorithm.exchange.managers.trade_manager import TradeManager

        self.position_manager = PositionManager(self)
        self.order_manager = OrderManager(self)
        self.trade_manager = TradeManager(self)

    @abstractmethod
    async def sync_trades(self):
        """
        Must implement a method that fetches trades from the exchange.
        Convert the trade information to a Trade object and register it by calling self.trade_manager.add().
        See CCXTExchange for more details.
        """
        pass

    @abstractmethod
    async def sync_orders(self, adhoc: bool = False):
        """
        Must implement a method that fetches updates for all "OPEN" orders and updates the "filled" and "status" properties.
        Pass order id, filled and status to self.order_manager.on_order_update().
        """
        pass

    @abstractmethod
    async def _create_limit_order(self, order: Order) -> str:
        """
        Must implement a method that creates a limit order, returns the server ID. See CCXTExchange for more details.
        """
        pass

    @abstractmethod
    async def _create_market_order(self, order: Order) -> str:
        """
        Must implement a method that creates a market order, returns the server ID. See CCXTExchange for more details.
        """
        pass

    @abstractmethod
    async def _cancel_order(self, order: Order) -> bool:
        """
        A method that cancels the order on the exchange and if successful updates the order status. See CCXTExchange for more details.
        """
        pass

    @abstractmethod
    async def _cancel_all_orders(self, pair: str = None) -> bool:
        """
        A method that cancels all orders on the exchange and if successful updates the order statuses. See CCXTExchange for more details.
        """
        pass

    @abstractmethod
    async def _get_balances(self) -> Balances:
        """
        A method to get the balances from the exchange. See CCXTExchange for more details.
        """
        pass

    @property
    @abstractmethod
    def exchange_type(self) -> ExchangeType:
        """
        Subclass must specify what type it represents (LIVE or PAPER).
        """
        pass

    async def open(
        self,
        pair: str,
        position_type: PositionType,
        quantity: float,
        order_type: OrderType,
        price: float = 0,
    ) -> Optional[Order]:
        """
        Asynchronously creates and submits a new market or limit order.

        Args:
            pair (str): Trading pair (e.g., "BTC/USDT").
            position_type (PositionType): Type of position (long or short).
            quantity (float): Amount to trade.
            order_type (OrderType): Type of order (limit or market).
            price (float, optional): Limit price. Ignored for market orders. Defaults to 0.

        Returns:
            Order or None: The created `Order` if successful, else `None`.

        Notes:
            - Uses `_get_highest_seen_timestamp` for backtest compatibility.
        """

        order = Order(
            timestamp=get_highest_timestamp(),
            pair=pair,
            position_type=position_type,
            quantity=quantity,
            price=price,
            order_type=order_type,
            trade_type=TradeType.OPEN,
        )
        self.order_manager.add_order(order)

        if order_type == OrderType.LIMIT:
            order.server_order_id = await self._create_limit_order(order)
        elif order_type == OrderType.MARKET:
            order.server_order_id = await self._create_market_order(order)

        if order.server_order_id is None:
            order.order_status = OrderStatus.REJECTED
            await self.order_manager.dispatch_deferred(
                order.client_order_id, OrderStatus.REJECTED
            )

            log_message(f"could not set server_order_id for {order.pair}.", "WARN")

        return order

    async def close(
        self,
        pair: str,
        position_type: PositionType,
        quantity: float,
        order_type: OrderType,
        price: float = 0,
    ) -> Optional[Order]:
        order = Order(
            timestamp=get_highest_timestamp(),
            pair=pair,
            position_type=position_type,
            quantity=quantity,
            price=price,
            order_type=order_type,
            trade_type=TradeType.CLOSE,
        )

        self.order_manager.add_order(order)

        if order_type == OrderType.LIMIT:
            order.server_order_id = await self._create_limit_order(order)
        elif order_type == OrderType.MARKET:
            order.server_order_id = await self._create_market_order(order)

        if order.server_order_id is None:
            order.order_status = OrderStatus.REJECTED

            await self.order_manager.dispatch_deferred(
                order.client_order_id, OrderStatus.REJECTED
            )

            log_message(f"could not set server_order_id for {order.pair}", "WARN")

        return order

    async def cancel_order(self, order: Order) -> bool:
        return await self._cancel_order(order)

    async def cancel_all_orders(self, pair: str = None) -> bool:
        return await self._cancel_all_orders(pair)

    async def get_balances(self) -> Balances:
        return await self._get_balances()
