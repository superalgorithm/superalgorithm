import asyncio
from typing import Any, Dict, Optional
import ccxt.pro as ccxt
from ccxt import Exchange
from ccxt import OperationFailed, OrderNotFound, RateLimitExceeded
from superalgorithm.exchange.base_exchange import BaseExchange
from superalgorithm.utils.helpers import (
    get_exponential_backoff_delay,
    get_now_ts,
)
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
from superalgorithm.utils.config import config


class CCXTExchange(BaseExchange):

    def __init__(self, exchange_id: str, params: Any = {}):

        super().__init__()

        self.TRADE_LOOKBACK_SECONDS = config.get("TRADE_LOOKBACK_SECONDS", 60)
        self.ORDER_LOOKBACK_SECONDS = config.get("ORDER_LOOKBACK_SECONDS", 600)
        self.POLL_INTERVAL_SECONDS = config.get("POLL_INTERVAL_SECONDS", 60)

        self.ccxt_client: Exchange = getattr(ccxt, exchange_id)(params)

        self.trade_queue = asyncio.Queue()
        self.order_queue = asyncio.Queue()

        self.register_task(self.sync_trades)
        self.register_task(self.process_trades)
        self.register_task(self.sync_orders)
        self.register_task(self.process_orders)
        self.register_task(self.poll_sync)

    def _json_to_trade_obj(self, trade_json):
        """
        This method parses the trade json the exchange returns a Trade object.
        """

        # NOTE: position_type and trade_type are not available in the trade response
        # from CCXT, we update this later in the _match_trade_with_order method
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
        Starts the continous loading routine to receive trade updates via socket.
        """
        attempt = 1

        while True:
            try:
                trades = await self.ccxt_client.watch_my_trades()
                for trade_json in trades:
                    self.trade_queue.put_nowait(trade_json)

            except Exception as e:
                log_exception(e, "CCXT Error watching trades.")
                # Exponential backoff with a maximum delay before re-connecting
                delay = get_exponential_backoff_delay(attempt)
                await asyncio.sleep(delay)

    async def process_trades(self):
        """
        Pop a trade from the queue and add it to the trade manager.
        """
        while True:
            trade_json = await self.trade_queue.get()
            trade = self._json_to_trade_obj(trade_json)
            await self.trade_manager.add(trade)

    async def sync_orders(self):
        """
        Continously monitor orders via web socket to sync order status and filled qty.
        """
        attempt = 1

        while True:
            try:
                orders = await self.ccxt_client.watch_orders()
                for order_json in orders:
                    self.order_queue.put_nowait(order_json)
            except Exception as e:
                log_exception(e, "Error syncing orders")
                # Exponential backoff with a maximum delay before re-connecting
                delay = get_exponential_backoff_delay(attempt)
                await asyncio.sleep(delay)

    async def process_orders(self):
        """
        Pop an order from the queue and update the order manager.
        """
        while True:
            order_json = await self.order_queue.get()

            if order_json is not None:
                if not order_json["clientOrderId"]:
                    server_order_id = order_json["id"]
                    order = self.order_manager.get_order_by_server_id(server_order_id)
                    client_order_id = order.client_order_id
                else:
                    client_order_id = int(order_json["clientOrderId"])

                filled = float(order_json.get("filled", 0))
                order_status = OrderStatus.parse_order_status(order_json["status"])

                asyncio.create_task(
                    self.order_manager.on_order_update(
                        client_order_id,
                        filled,
                        order_status,
                    )
                )

    async def poll_sync(self):
        """
        API version that polls the trades and orders via API endpoints.
        Used as a backup in case the websocket connection fails or missing trades.
        Default load trades within the last minute, orders within last 10 minutes.
        Can be adjusted via config (TRADE_LOOKBACK_SECONDS, ORDER_LOOKBACK_SECONDS).
        """
        trade_fetch_time = get_now_ts()
        order_fetch_time = get_now_ts()

        while True:
            try:
                await self.fetch_trades(since_timestamp=trade_fetch_time)
                await self.fetch_orders(since_timestamp=order_fetch_time)

                trade_fetch_time = get_now_ts() - 1000 * self.TRADE_LOOKBACK_SECONDS
                order_fetch_time = get_now_ts() - 1000 * self.ORDER_LOOKBACK_SECONDS
            except Exception as e:
                log_exception(e, "CCXT Error syncing trades and orders.")

            await asyncio.sleep(self.POLL_INTERVAL_SECONDS)

    async def fetch_trades(self, since_timestamp):
        trades = await self.ccxt_client.fetchMyTrades(since=since_timestamp)
        for trade_json in trades:
            self.trade_queue.put_nowait(trade_json)

    async def fetch_orders(self, since_timestamp: int):
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

            log_message(f"CCXT _create_limit_order response {response}", stdout=False)
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

            log_message(f"CCXT _create_market_order response {response}", stdout=False)
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
                await self.order_manager.on_order_update(
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
            if order.order_status != OrderStatus.OPEN:
                log_message(
                    f"Order {order.server_order_id} is not open, skipping cancel",
                    "INFO",
                )
                return True

            if await self._cancel_order(order):
                return True
            else:

                delay = get_exponential_backoff_delay(attempt)
                log_message(
                    f"Retry cancel order {order.server_order_id} in {delay:.2f} sec.",
                    "INFO",
                )
                attempt += 1

        await self.order_manager.on_order_update(
            order.client_order_id, order.filled, OrderStatus.EXPIRED
        )
        log_message(
            f"Cancel order failed {order.server_order_id} after {max_retries} attempts",
            "ERROR",
        )
        return False

    async def _cancel_all_orders(self, pair: str = None) -> bool:
        """
        Cancels all open orders, optionally filtered by a specific trading pair.

        Notes:
            - Cancels each order individually via _cancel_order,
              and may trigger rate limits, but is preferred as CCXT
              returns inconsitent responses per exchange.
            - also this way we can cancel all orders managed by
              the strategy vs. all orders on the account.
        """
        success = []

        for order in list(self.order_manager.orders.values()):
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
        CCXT helper method to get the current account balances for each currency
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
        Helper method to get the side of the order based on its position and trade type.
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
