import asyncio
from typing import Dict
import uuid
import warnings

# from superalgorithm.exchange.position import Position
from superalgorithm.exchange.status_tracker import (
    get_highest_timestamp,
    get_latest_price,
)
from superalgorithm.types.data_types import (
    BalanceData,
    Balances,
    ExchangeType,
    Order,
    OrderStatus,
    Trade,
    PositionType,
    TradeType,
    Position,
)
from superalgorithm.exchange.base_exchange import BaseExchange, InsufficientFundsError
from superalgorithm.utils.logging import log_message, log_order
from superalgorithm.exchange.status_tracker import get_latest_price


class PaperExchange(BaseExchange):
    def __init__(self, initial_cash: int = 10000):
        super().__init__()
        self.initial_cash = initial_cash  # required by backtest analytics
        self.cash = initial_cash  # cash balance of the paper account
        self.trade_tasks = []

    async def _simulate_trade_execution(self, trade_response_json: Dict):
        """
        Simulates parallel trade execution. `trade_response_server_order_id` mimics the server's response with trade info.

        When creating a `Trade` object, the associated order is found using the `server_order_id` from the orders API. To avoid race conditions in live trading—where a trade might be reported before the orders API finishes—we must `_confirm_order_creation`.
        """
        associated_order = self.get_order_by_server_id(
            trade_response_json.get("server_order_id")
        )

        if associated_order is None:
            warnings.warn(
                f"got trade with order id {trade_response_json.get('server_order_id')}, but cannot find order"
            )
            return None

        trade = Trade(
            trade_id=trade_response_json.get("trade_id"),
            timestamp=trade_response_json.get("timestamp"),
            pair=trade_response_json.get("pair"),
            position_type=None,
            trade_type=None,
            price=trade_response_json.get("price"),
            quantity=trade_response_json.get("quantity"),
            server_order_id=trade_response_json.get("server_order_id"),
        )

        if trade.trade_id not in self.processed_trades:
            await self.trade_queue.put(trade)
            self.processed_trades.add(trade.trade_id)

        # here we wait until the trade is processed by the base exchange
        # base exchange will take the trade of the queue and try to match it with the order
        # if the order was found, the trade will be added to the position
        await self.trade_queue.join()

        self._update_cash(trade)

        # manually update the order status and dispatch the order status event
        # in a live exchange await self._load_orders_forever(True) would have been called by the base exchange once the trade was matched to an order
        # however, in the paper exchange world there will be no trades without corresponding order, hence this will work and avoids us having to simulate fetching order updates from an API

        associated_order.filled += trade.quantity
        if associated_order.filled == associated_order.quantity:
            associated_order.order_status = OrderStatus.CLOSED

        log_order(associated_order)

        associated_order.dispatch(associated_order.order_status.value, associated_order)

    async def _create_limit_order(self, order: Order) -> str:

        # ensure order will be accepted by the exchange and we have anough cash and balances
        self._validate_order_execution(order)

        # create a order id like a real exchange would
        server_order_id = str(uuid.uuid4())

        # create a trade object to simulate the response from the exchange trade,
        # this is the minimum information required to create a trade
        trade_json = {
            "trade_id": str(uuid.uuid4()),
            "timestamp": get_highest_timestamp(),
            "pair": order.pair,
            "price": order.price,
            "quantity": order.quantity,
            "server_order_id": server_order_id,
        }

        # Simulating order exection. In a live exchage environment the order would be placed on the exchange and self._trade_fetch would have to listen for new trades filling the order.
        self.trade_tasks.append(
            asyncio.create_task(self._simulate_trade_execution(trade_json))
        )

        return server_order_id

    async def _create_market_order(self, order: Order) -> str:
        # This doesn't do anything really but store the current mark price on the order, and create a limit order. PaperExchange processes all limit orders anyways...
        mark_price = get_latest_price(order.pair)
        order.price = mark_price.mark
        return await self._create_limit_order(order)

    async def _load_trades_forever(self):
        # not implemented for paper trade, PaperExchange calls self._on_trade(trade) directly
        pass

    async def _load_orders_forever(self, adhoc: bool = False):
        # not implemented for paper trade, PaperExchange calls updates order status directly once trade simulation ends
        pass

    async def _cancel_order(self, order: Order) -> bool:
        # not implemented as right now orders go through instantly
        pass

    async def _cancel_all_orders(self, pair: str = None) -> bool:
        # not implemented as right now orders go through instantly
        pass

    def _get_short_debt(self):
        """
        Computes the total value of all short positions.
        """
        return sum(
            position.balance * position.average_open
            for pair in self.positions
            for position in self.positions[pair].values()
            if position.position_type == PositionType.SHORT
        )

    def _validate_order_execution(self, order: Order):
        """
        Validates order execution by checking if there is enough capital and balance to execute the order.
        """
        required_cash = order.quantity * order.price
        total_debt = self._get_short_debt()
        trade_type = order.trade_type
        position_type = order.position_type

        if trade_type == TradeType.OPEN and position_type == PositionType.LONG:
            if self.cash < required_cash:
                raise InsufficientFundsError("Insufficient cash to open position")
        if trade_type == TradeType.CLOSE and position_type == PositionType.LONG:
            if self.positions[order.pair][order.position_type].balance < order.quantity:
                raise InsufficientFundsError(
                    f"Insufficient balance for {order.pair} to close position"
                )
        if trade_type == TradeType.OPEN and position_type == PositionType.SHORT:
            if required_cash + total_debt > self.cash:
                raise InsufficientFundsError(
                    f"Your shorting too much! SHORT {order.pair}. Transaction requires additional {required_cash}, already short {total_debt}, cash in account {self.cash}."
                )

        if trade_type == TradeType.CLOSE and position_type == PositionType.SHORT:
            if self.positions[order.pair][order.position_type].balance < order.quantity:
                raise InsufficientFundsError(
                    f"Insufficient balance for {order.pair} to close position"
                )

        if order.price != get_latest_price(order.pair).mark:
            log_message(
                f"The order price and mark price do not match. {order.pair} {order.price}, {get_latest_price(order.pair).mark}",
                "WARN",
            )

    def _update_cash(self, trade: Trade):
        """
        This method updates the paper trade cash balance after a trade was executed
        """

        associated_order = self.get_order_by_server_id(trade.server_order_id)

        # based on the trade, find the position against which we traded
        position: Position = self.positions[associated_order.pair][
            associated_order.position_type
        ]

        # get the trade from our Position, which now contains the computed pnl for that trade
        updated_trade = position.get_trade(trade_id=trade.trade_id)

        trade_type = updated_trade.trade_type
        position_type = associated_order.position_type
        price = updated_trade.price
        quantity = updated_trade.quantity
        pnl = updated_trade.pnl

        # compute the new cash balance based on the trade and position type (close long, open short etc.)
        if trade_type == TradeType.OPEN and position_type == PositionType.LONG:
            self.cash -= price * quantity
        elif trade_type == TradeType.CLOSE and position_type == PositionType.LONG:
            self.cash += price * quantity
        elif trade_type == TradeType.CLOSE and position_type == PositionType.SHORT:
            self.cash += pnl

    async def _get_balances(self) -> Balances:
        balances = Balances()
        balances.free["USD"] = self.cash

        for pair in self.positions:
            for position in self.positions[pair].values():
                if position.position_type == PositionType.LONG:
                    balances.free[position.pair] = position.balance

                balances.currencies[pair] = BalanceData(
                    position.balance, 0, position.balance, 0
                )

        return balances

    @property
    def exchange_type(self) -> ExchangeType:
        return ExchangeType.PAPER
