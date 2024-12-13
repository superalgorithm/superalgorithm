from typing import Dict, List
from superalgorithm.types.data_types import Position, PositionType, Trade, TradeType
from superalgorithm.utils.logging import log_message, log_position
from superalgorithm.exchange.base_exchange import BaseExchange

PositionDict = Dict[str, Dict[str, Position]]


class PositionManager:
    def __init__(self, exchange: BaseExchange):
        self.exchange = exchange
        self.positions: PositionDict = {}

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

    def add_trade(self, trade: Trade):
        position = self.get_or_create_position(trade.pair, trade.position_type)
        position.add_trade(trade)
        log_position(position, stdout=False)

    # todo: re-evaluatie this method need
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
