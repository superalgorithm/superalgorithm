import asyncio
from typing import Any, Dict, Optional
import ccxt.pro as ccxt
from ccxt import Exchange
from superalgorithm.exchange.base_exchange import BaseExchange
from superalgorithm.utils.helpers import get_now_ts
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
        self.ccxt_client: Exchange = getattr(ccxt, exchange_id)(config)

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

    async def _process_trade(self, trade_json):
        """
        We add any trade we have not seen before to the trade queue. From there the base exchange will try and find the associated order, if any.
        """
        trade = self._json_to_trade_obj(trade_json)

        if trade.trade_id not in self.processed_trades:
            await self.trade_queue.put(trade)
            self.processed_trades.add(trade.trade_id)

    async def _watch_trades(self):
        """
        Websocket method, listens to the trades executed on the exchange.
        """
        while True:
            try:
                trades = await self.ccxt_client.watchMyTrades()
                for trade_json in trades:
                    await self._process_trade(trade_json)

            except Exception as e:
                log_exception(e, "CCXT Error watching trades.")

            await asyncio.sleep(1)

    async def _fetch_trades(self):
        """
        API version that polls the API endpoint, loads all recent trades and processes them. We use this if watchMyTrades is not available.
        """
        since_fetch = get_now_ts()

        while True:
            try:
                trades = await self.ccxt_client.fetchMyTrades(since=since_fetch)
                for trade_json in trades:
                    await self._process_trade(trade_json)
                since_fetch = get_now_ts() - 1000 * 60 * 60  # 1 hour
            except Exception as e:
                log_exception(e, "CCXT Error fetching trades.")

            await asyncio.sleep(3)

    async def _load_trades_forever(self):
        """
        Starts the continous loading routine for receiving trade updates.
        """

        if self.ccxt_client.has["watchMyTrades"]:
            log_message("using socket to watch trades")
            await self._watch_trades()
        else:
            log_message("using polling to watch trades")
            await self._fetch_trades()

    async def _fetch_orders(self):
        """
        For each open order, we fetch an update from the exchange to see if the order was closed.
        """
        for order in list(self.orders.values()):
            if order.order_status == OrderStatus.OPEN:
                response = await self.ccxt_client.fetchOrder(order.server_order_id)
                order.filled = response["filled"]
                if order.order_status != OrderStatus.parse_order_status(
                    response["status"]
                ):
                    order.order_status = OrderStatus.parse_order_status(
                        response["status"]
                    )
                    order.dispatch(order.order_status.value, order)

            await asyncio.sleep(0.5)  # avoid rate limits

    async def _load_orders_forever(self, adhoc: bool = False):
        """
        Continously calls _fetch_orders every 60 seconds to sync the local order status with the orders on the exchange.
        """
        if adhoc:
            await self._fetch_orders()
        else:
            while True:
                try:
                    await self._fetch_orders()
                except Exception as e:
                    log_exception(e, f"Error fetching orders forever:")
                await asyncio.sleep(60)

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
        response = await self.ccxt_client.cancelOrder(
            order.server_order_id, symbol=order.pair
        )

        log_message(f"CCXT _cancel_order response {response}")

        if response["status"] == "canceled":
            order.order_status = OrderStatus.CANCELED
            order.dispatch(OrderStatus.CANCELED.value, order)
            return True
        return False

    async def _cancel_all_orders(self, pair: str = None) -> bool:
        """
        Cancels all open orders, optionally filtered by a specific trading pair.

        Notes:
            - Cancels each order individually via _cancel_order, and may trigger rate limits, but is preferred as CCXT returns inconsitent responses per exchange.
        """
        success = []
        for order in self.orders.values():
            if (
                pair is None or order.pair == pair
            ) and order.order_status == OrderStatus.OPEN:
                success.append(await self.cancel_order(order))
                await asyncio.sleep(1)
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
