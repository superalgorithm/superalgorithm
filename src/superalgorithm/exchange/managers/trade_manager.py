import asyncio
from typing import List
from superalgorithm.exchange.base_exchange import BaseExchange
from superalgorithm.types.data_types import PositionType, Trade, TradeType
from superalgorithm.utils.event_emitter import EventEmitter
from superalgorithm.utils.logging import log_message, log_trade


class TradeManager(EventEmitter):
    def __init__(self, exchange: BaseExchange):
        super().__init__()
        self.exchange = exchange
        self.processed_trades = set()
        self.trades: List[Trade] = []

    def trade_matching(self, trade: Trade):
        """
        Checks if the trade belongs to one of the orders created by this system, because we may receive trades in the socket update from other systems or manual trades.
        """

        associated_order = self.exchange.order_manager.get_order_by_server_id(
            trade.server_order_id
        )

        if associated_order:
            # update the trade with the order's pair
            trade.position_type = associated_order.position_type
            trade.trade_type = associated_order.trade_type
            self.trades.append(trade)

            # add the trade to the position manager
            self.exchange.position_manager.add_trade(trade)

            # add the trade to the order as reference
            associated_order.add_trade(trade)

            log_trade(trade, stdout=False)

            self.dispatch("trade", trade)

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

    def list_trades(
        self,
        pair: str = None,
        trade_type: TradeType = None,
        position_type: PositionType = None,
    ) -> List[Trade]:
        """
        Retrieves all trades with optional filtering by trade type and position type.
        """
        if not self.trades:
            return []

        return [
            trade
            for trade in self.trades
            if (pair is None or trade.pair == pair)
            and (trade_type is None or trade.trade_type == trade_type)
            and (position_type is None or trade.position.position_type == position_type)
        ]

    def get_trade(self, trade_id: str) -> Trade:
        """
        Retrieves a trade by id.
        """
        for trade in self.trades:
            if trade.trade_id == trade_id:
                return trade
        return None
