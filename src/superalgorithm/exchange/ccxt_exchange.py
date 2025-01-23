import asyncio
from typing import Any, Dict, Optional
import ccxt.pro as ccxt
from ccxt import Exchange
from ccxt import OperationFailed, OrderNotFound, RateLimitExceeded
from superalgorithm.exchange.base_exchange import BaseExchange
from superalgorithm.utils.helpers import get_exponential_backoff_delay, get_now_ts
from superalgorithm.utils.logging import log_exception, log_message
from superalgorithm.types.data_types import (
    BalanceData,
    Balances,
    ExchangeType,
    Order,
    OrderStatus,
    PositionType,
    Trade,
    TradeType,
)


class CCXTExchange(BaseExchange):

    def __init__(self, exchange_id: str, config: Any = {}):

        super().__init__()

        self.TRADE_LOOKBACK_SECONDS = config.get("TRADE_LOOKBACK_SECONDS", 10)
        self.ORDER_LOOKBACK_SECONDS = config.get("ORDER_LOOKBACK_SECONDS", 600)
        self.POLL_INTERVAL_SECONDS = config.get("POLL_INTERVAL_SECONDS", 10)

        self.ccxt_client: Exchange = getattr(ccxt, exchange_id)(config)

        self.trade_queue = asyncio.Queue()
        self.order_queue = asyncio.Queue()

        self.register_task(self.sync_trades)
        self.register_task(self.process_trades)
        self.register_task(self.sync_orders)
        self.register_task(self.process_orders)
        self.register_task(self.poll_sync)

    def _json_to_trade_obj(self, trade_json):
        """
        This method parses the trade information received from the server and creates a Trade object.
        """

        # NOTE: position_type and trade_type are not available in the trade response from CCXT, we update this later in the _match_trade_with_order method
        trade = Trade(
            trade_id=trade_json["id"],
            timestamp=trade_json["timestamp"],
            pair=trade_json["symbol"],
            position_type=None,
            trade_type=None,
            price=trade_json["price"],
            quantity=trade_json["amount"],
            server_order_id=trade_json["order"],
        )

        return trade

    async def sync_trades(self):
        """
        Starts the continous loading routine for receiving trade updates.
        """

        while True:
            try:
                trades = await self.ccxt_client.watch_my_trades()
                for trade_json in trades:
                    self.trade_queue.put_nowait(trade_json)

            except Exception as e:
                log_exception(e, "CCXT Error watching trades.")
                await asyncio.sleep(
                    1
                )  # sleep to not throw the same error in a while loop

    async def process_trades(self):
        while True:
            trade_json = await self.trade_queue.get()
            await self.trade_manager.add(self._json_to_trade_obj(trade_json))

    async def sync_orders(self):
        """
        Continously monitor orders to sync the local order status with the orders on the exchange.
        """
        while True:
            try:
                orders = await self.ccxt_client.watch_orders()
                for order_json in orders:
                    self.order_queue.put_nowait(order_json)
            except Exception as e:
                log_exception(e, f"Error syncing orders")
                await asyncio.sleep(
                    1
                )  # sleep to not throw the same error in a while loop

    async def process_orders(self):
        while True:
            order_json = await self.order_queue.get()

            # skip a beat to we allow the trade for this order to be received and processed i.e. updating/creating the position before the order dispatches any events.
            # this a race condition between receiving order updates and trade updates, we may receive the order update before the trade update.
            # this matters if we would listen to the order CLOSED event and expect the position to be updated already with the trade information.
            await asyncio.sleep(1)

            self.order_manager.on_order_update(
                int(order_json["clientOrderId"]),
                order_json["filled"],
                OrderStatus.parse_order_status(order_json["status"]),
            )

    async def poll_sync(self):
        """
        API version that polls the API endpoints, loads all recent trades and and orders and processes them.
        Used as a backup in case the websocket connection fails.
        """
        last_trade_fetch_time = get_now_ts()
        last_order_fetch_time = get_now_ts()

        while True:
            try:
                await self.fetch_and_process_trades(
                    since_timestamp=last_trade_fetch_time
                )
                await self.fetch_and_process_orders(
                    since_timestamp=last_order_fetch_time
                )

                last_trade_fetch_time = (
                    get_now_ts() - 1000 * self.TRADE_LOOKBACK_SECONDS
                )
                last_order_fetch_time = (
                    get_now_ts() - 1000 * self.ORDER_LOOKBACK_SECONDS
                )
            except Exception as e:
                log_exception(e, "CCXT Error syncing trades and orders.")

            await asyncio.sleep(self.POLL_INTERVAL_SECONDS)

    async def fetch_and_process_trades(self, since_timestamp):
        trades = await self.ccxt_client.fetchMyTrades(since=since_timestamp)
        for trade_json in trades:
            self.trade_queue.put_nowait(trade_json)

    async def fetch_and_process_orders(self, since_timestamp):
        orders = await self.ccxt_client.fetchOrders(since=since_timestamp)
        for order_json in orders:
            self.order_queue.put_nowait(order_json)

    async def _create_limit_order(
        self, order: Order, params: Optional[Dict] = None
    ) -> str:
        """
        Creates the limit order on the exchange and returns the server order id.
        """
        try:

            response = await self.ccxt_client.createOrder(
                symbol=order.pair,
                type="limit",
                side=self._get_order_side(order),
                amount=order.quantity,
                price=order.price,
                params={} if params is None else params,
            )

            log_message(f"CCXT _create_limit_order response {response}", stdout=True)
            return response["id"]
        except Exception as e:
            log_exception(e, "Error creating limit order", stdout=True)
            return None

    async def _create_market_order(
        self, order: Order, params: Optional[Dict] = None
    ) -> str:
        """
        Creates the market order on the exchange and returns the server order id.
        """
        try:
            response = await self.ccxt_client.createOrder(
                symbol=order.pair,
                type="market",
                side=self._get_order_side(order),
                amount=order.quantity,
                params={} if params is None else params,
            )

            log_message(f"CCXT _create_market_order response {response}")
            return response["id"]
        except Exception as e:
            log_exception(e, "Error creating market order")
            return None

    async def _cancel_order(self, order: Order) -> bool:
        """
        Cancels an order on the exchange and updates the status.
        """
        try:
            response = await self.ccxt_client.cancelOrder(
                order.server_order_id, symbol=order.pair
            )
            if response["status"] == "canceled":
                self.order_manager.on_order_update(
                    order.client_order_id, order.filled, OrderStatus.CANCELED
                )
                return True

            return False

        except OrderNotFound:
            log_message(f"OrderNotFound when cancelling order {order.server_order_id}")
            return False

        except OperationFailed as e:
            log_exception(
                e, f"OperationFailed when cancelling order {order.server_order_id}"
            )
            return False

        except Exception as e:
            log_exception(
                e, f"Unexpected error when cancelling order {order.server_order_id}"
            )
            return False

    async def _cancel_with_retry(self, order: Order) -> bool:
        """
        Cancels an order on the exchange and retries in case of an error.
        """
        max_retries = 2
        attempt = 1

        while attempt <= max_retries:
            if await self._cancel_order(order):
                return True
            else:
                delay = get_exponential_backoff_delay(attempt)
                log_message(
                    f"Retrying cancel order {order.server_order_id} in {delay:.2f} seconds",
                    "INFO",
                )
                await asyncio.sleep(delay)
                attempt += 1

        self.order_manager.on_order_update(
            order.client_order_id, order.filled, OrderStatus.EXPIRED
        )
        log_message(
            f"Failed to cancel order {order.server_order_id} after {max_retries} attempts",
            "ERROR",
        )
        return False

    async def _cancel_all_orders(self, pair: str = None) -> bool:
        """
        Cancels all open orders, optionally filtered by a specific trading pair.

        Notes:
            - Cancels each order individually via _cancel_order, and may trigger rate limits, but is preferred as CCXT returns inconsitent responses per exchange.
            - also this way we can cancel all orders managed by the strategy vs. all orders on the account.
        """
        success = []
        for order in self.order_manager.orders.values():
            if (
                pair is None or order.pair == pair
            ) and order.order_status == OrderStatus.OPEN:
                try:
                    result = await self._cancel_with_retry(order)
                    success.append(result)
                except RateLimitExceeded as e:
                    log_exception(e, "RateLimitExceeded when cancelling all orders")
                    await asyncio.sleep(5)
                    result = await self._cancel_with_retry(order)
                    success.append(result)
                await asyncio.sleep(0.1)
        if not all(success):
            log_message("Some orders failed to cancel", "WARN")
        return all(success)

    async def _get_balances(self) -> Balances:
        """
        CCXT specific helper method to get the current account balances for each currency
        See: https://docs.ccxt.com/#/?id=balance-structure
        """
        balance = await self.ccxt_client.fetchBalance()

        response = Balances()

        for currency, data in balance.items():

            if currency not in ["free", "used", "total", "debt", "info"]:

                response.free[currency] = data.get("free", 0)
                response.used[currency] = data.get("used", 0)
                response.total[currency] = data.get("total", 0)
                response.debt[currency] = data.get("debt", 0)

                response.currencies[currency] = BalanceData(
                    data.get("free", 0),
                    data.get("used", 0),
                    data.get("total", 0),
                    data.get("debt", 0),
                )

        return response

    def _get_order_side(self, order: Order):
        """
        Helper method to determine the side of the order based on its position and trade type.
        """
        side = (
            "buy"
            if (
                (
                    order.position_type == PositionType.LONG
                    and order.trade_type == TradeType.OPEN
                )
                or (
                    order.position_type == PositionType.SHORT
                    and order.trade_type == TradeType.CLOSE
                )
            )
            else "sell"
        )
        return side

    @property
    def exchange_type(self) -> ExchangeType:
        return ExchangeType.LIVE
