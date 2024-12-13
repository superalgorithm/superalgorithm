import asyncio
from typing import List
from superalgorithm.exchange.base_exchange import BaseExchange
from superalgorithm.types.data_types import Trade
from superalgorithm.utils.logging import log_message, log_trade


class TradeManager:
    def __init__(self, exchange: BaseExchange):
        self.exchange = exchange
        self.processed_trades = set()

    def trade_matching(self, trade: Trade):
        """
        Checks if the trade belongs to one of the orders created by this system, because we may receive trades in the socket update from other systems or manual trades.
        """
        associated_order = self.exchange.order_manager.get_order_by_server_id(
            trade.server_order_id
        )

        if associated_order:
            trade.position_type = associated_order.position_type
            trade.trade_type = associated_order.trade_type
            self.exchange.position_manager.add_trade(trade)
            log_trade(trade, stdout=False)
            return True

        return False

    async def add(self, trade: Trade):
        if trade.trade_id in self.processed_trades:
            return

        self.processed_trades.add(trade.trade_id)

        if not self.trade_matching(trade):
            await self.retry_trade_matching(trade)

    async def retry_trade_matching(self, trade: Trade):
        await asyncio.sleep(1)
        if not self.trade_matching(trade):
            log_message(
                f"got trade with order id {trade.server_order_id}, but cannot find order",
                "WARN",
            )
