from abc import abstractmethod
import asyncio
from typing import Dict, List
from superalgorithm.exchange.status_tracker import get_highest_timestamp
from superalgorithm.types.data_types import (
    ExchangeType,
    Order,
    OrderType,
    OrderStatus,
    Trade,
    TradeType,
    PositionType,
    Balances,
    Position,
)
from superalgorithm.utils.event_emitter import EventEmitter
from superalgorithm.utils.logging import (
    log_message,
    log_order,
    log_position,
    log_trade,
)


class InsufficientFundsError(Exception):
    """Exception raised when there are insufficient funds to open a position."""

    pass


PositionDict = Dict[str, Dict[str, Position]]


class BaseExchange(EventEmitter):

    def __init__(self):
        super().__init__()
        self._async_tasks = []
        self.orders: Dict[str, Order] = {}
        self.positions: PositionDict = {}
        self.trade_queue = asyncio.Queue()
        self.processed_trades = set()

    @abstractmethod
    async def _load_trades_forever(self):
        """
        Must implement a method that fetches trades from the exchange.
        Convert the trade information to a Trade object and add it to the self.trade_queue.
        Mark the trade as processed by adding it to self.processed_trades.
        See CCXTExchange for more details.
        """
        pass

    @abstractmethod
    async def _load_orders_forever(self, adhoc: bool = False):
        """
        Must implement a method that continously fetches updates for all "OPEN" orders and updates the "filled" and "status" properties.
        If the status has changed, dispatch the appropriate event.
        The method has to support adhoc updates, e.g., triggered by a manual refresh.
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

    async def _match_trade_with_order(self, trade: Trade):
        """
        Matches an incoming trade with a local order and calls _on_trade if a match is found.
        Why: exchange instances may receive trades that they have not placed themselves, e.g., from another system or manual trading.
        """
        associated_order = self.get_order_by_server_id(trade.server_order_id)

        if associated_order:
            trade.position_type = associated_order.position_type
            trade.trade_type = associated_order.trade_type
            self._on_trade(trade)
            self.trade_queue.task_done()
            log_trade(trade)
            await self._load_orders_forever(True)
            return True

        return False

    async def _process_trades(self):
        """
        Processes trades from the trade queue and matches them with orders.
        """
        while True:
            trade = await self.trade_queue.get()
            if await self._match_trade_with_order(trade):
                continue
            else:
                # sleep for a second and try again, if we still don't have a matching order we assume this trade is from another system
                await asyncio.sleep(1)
                if await self._match_trade_with_order(trade):
                    continue
                else:
                    log_message(
                        f"got trade with order id {trade.server_order_id}, but cannot find order",
                        "WARN",
                    )
                    self.trade_queue.task_done()

            await asyncio.sleep(1)

    def _on_trade(self, trade: Trade):
        """
        Called by `BaseExchange` subclasses for all new trades matched to one of our orders.
        """

        position = self.get_or_create_position(trade.pair, trade.position_type)
        position.add_trade(trade)
        log_position(position)

        # NOTE: one or more trades result in a filled order and hence the order status changes. Each exchange implementation should dispatch order status events. See sample exchange implementations for details.

    @property
    @abstractmethod
    def exchange_type(self) -> ExchangeType:
        """
        Subclass must specify what type it represents (LIVE or PAPER).
        """
        pass

    def get_order_by_server_id(self, server_order_id: str):
        for order in self.orders.values():
            if order.server_order_id == server_order_id:
                return order
        return None  # Return None if no order with the given ID is found

    def get_or_create_position(
        self, pair: str, position_type: PositionType
    ) -> Position:
        """
        Retrieves an existing position or creates a new one if it does not exist.
        """
        if pair not in self.positions:
            self.positions[pair] = {}
        if position_type not in self.positions[pair]:
            self.positions[pair][position_type] = Position(pair, position_type)
            log_message(f"created new position for {pair} {position_type}", "INFO")
        return self.positions[pair][position_type]

    def list_trades(
        self, trade_type: TradeType = None, position_type: PositionType = None
    ) -> List[Trade]:
        """
        Retrieves all trades with optional filtering by trade type and position type.
        """
        if not self.positions:
            return []

        return [
            trade
            for pair_positions in self.positions.values()
            for position in pair_positions.values()
            if position_type is None or position.position_type == position_type
            for trade in position.trades
            if trade_type is None or trade.trade_type == trade_type
        ]

    def get_trade(self, trade_id: str) -> Trade:
        """
        Retrieves a trade by id.
        """
        if not self.positions:
            return None

        for pair_positions in self.positions.values():
            for position in pair_positions.values():
                for trade in position.trades:
                    if trade.trade_id == trade_id:
                        return trade
        return None

    async def open(
        self,
        pair: str,
        position_type: PositionType,
        quantity: float,
        order_type: OrderType,
        price: float = 0,
    ):
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

        self.orders[order.client_order_id] = order

        if order_type == OrderType.LIMIT:
            order.server_order_id = await self._create_limit_order(order)
        elif order_type == OrderType.MARKET:
            order.server_order_id = await self._create_market_order(order)

        if order.server_order_id is None:
            order.order_status = (
                OrderStatus.REJECTED
            )  # Note: this does not dispatch the "REJECTED" event only updates from the server dispatch the events
            log_message(f"could not set server_order_id for {order.pair}.", "WARN")

        log_order(order)
        return order

    async def close(
        self,
        pair: str,
        position_type: PositionType,
        quantity: float,
        order_type: OrderType,
        price: float = 0,
    ):
        order = Order(
            timestamp=get_highest_timestamp(),
            pair=pair,
            position_type=position_type,
            quantity=quantity,
            price=price,
            order_type=order_type,
            trade_type=TradeType.CLOSE,
        )

        self.orders[order.client_order_id] = order

        if order_type == OrderType.LIMIT:
            order.server_order_id = await self._create_limit_order(order)
        elif order_type == OrderType.MARKET:
            order.server_order_id = await self._create_market_order(order)

        if order.server_order_id is None:
            order.order_status = (
                OrderStatus.REJECTED
            )  # Note: this does not dispatch the "REJECTED" event only updates from the server dispatch the events
            # its a limitation at the moment as we return the order only once it was created or rejected
            # hence, any added event listener won't fire the event as it already happened
            # solution is either to return an order without server id or to dispatch the event through some queue
            log_message(f"could not set server_order_id for {order.pair}", "WARN")

        log_order(order)
        return order

    async def cancel_order(self, order: Order) -> bool:
        return await self._cancel_order(order)

    async def cancel_all_orders(self, pair: str = None) -> bool:
        return await self._cancel_all_orders(pair)

    async def get_balances(self) -> Balances:
        return await self._get_balances()

    def start(self):

        self._async_tasks = [
            asyncio.create_task(self._load_trades_forever()),
            asyncio.create_task(self._process_trades()),
            asyncio.create_task(self._load_orders_forever()),
        ]
        return self._async_tasks

    def stop(self):
        for task in self._async_tasks:
            task.cancel()
        self._async_tasks = []
