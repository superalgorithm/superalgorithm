from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
import json
import random
from typing import Any, Dict, List, Optional, Union
from superalgorithm.utils.event_emitter import EventEmitter
from superalgorithm.utils.helpers import get_now_ts


class ExchangeType(Enum):
    LIVE = "LIVE"
    PAPER = "PAPER"


class ExecutionMode(Enum):
    LIVE = "LIVE"
    PRELOAD = "PRELOAD"
    PAPER = "PAPER"


class TradeType(Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"


class PositionType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class OrderStatus(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"

    @staticmethod
    def parse_order_status(status_string: str) -> OrderStatus:
        status_mapping = {
            "open": OrderStatus.OPEN,
            "closed": OrderStatus.CLOSED,
            "canceled": OrderStatus.CANCELED,
            "expired": OrderStatus.EXPIRED,
            "rejected": OrderStatus.REJECTED,
        }

        return status_mapping.get(status_string.lower())


class OrderType(Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


@dataclass
class AggregatorResult:
    current_bar: OHLCV
    is_new_bar_started: bool
    last_completed_bar: Optional[OHLCV]


@dataclass
class OHLCV:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class MarkPrice:
    timestamp: int
    mark: float


class Order(EventEmitter):
    def __init__(
        self,
        pair: str,
        position_type: PositionType,
        trade_type: TradeType,
        quantity: float,
        price: float,
        order_type: OrderType,
        order_status: OrderStatus = OrderStatus.OPEN,
        client_order_id: Optional[int] = None,
        server_order_id: Optional[str] = None,
        timestamp: Optional[int] = None,
        filled: int = 0,
    ):
        super().__init__()
        self.pair = pair
        self.position_type = position_type
        self.trade_type = trade_type
        self.quantity = quantity
        self.price = price
        self.order_type = order_type
        self.order_status = order_status
        self.client_order_id = client_order_id or self.generate_client_id()
        self.server_order_id = server_order_id
        self.timestamp = timestamp or int(datetime.now().timestamp())
        self.filled = filled

    def to_dict(self):
        data_dict = {}
        data_dict["pair"] = self.pair
        data_dict["position_type"] = self.position_type.value
        data_dict["trade_type"] = self.trade_type.value
        data_dict["quantity"] = self.quantity
        data_dict["quantity"] = self.quantity
        data_dict["order_type"] = self.order_type.value
        data_dict["order_status"] = self.order_status.value
        data_dict["client_order_id"] = self.client_order_id
        data_dict["server_order_id"] = self.server_order_id
        data_dict["timestamp"] = self.timestamp
        data_dict["filled"] = self.filled
        return data_dict

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

    @staticmethod
    def generate_client_id() -> int:
        current_time = int(datetime.now().timestamp())  # Current time in milliseconds
        random_number = random.randint(0, 9999)  # Random number between 0 and 9999
        return current_time * 10000 + random_number  # Combine to form a unique integer


@dataclass
class Trade:
    trade_id: str
    timestamp: int
    pair: str
    position_type: PositionType
    trade_type: TradeType
    price: float
    quantity: float
    server_order_id: str
    pnl: int = 0

    def to_dict(self):
        data_dict = asdict(self)
        data_dict["position_type"] = self.position_type.value
        data_dict["trade_type"] = self.trade_type.value
        return data_dict

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)


@dataclass
class Bar:
    timestamp: int
    source_id: str
    timeframe: str
    ohlcv: OHLCV

    @property
    def open(self) -> float:
        return self.ohlcv.open

    @property
    def high(self) -> float:
        return self.ohlcv.high

    @property
    def low(self) -> float:
        return self.ohlcv.low

    @property
    def close(self) -> float:
        return self.ohlcv.close

    @property
    def volume(self) -> float:
        return self.ohlcv.volume


@dataclass
class BalanceData:
    free: float
    used: float
    total: float
    debt: float


@dataclass
class Balances:
    free: Dict[str, float] = field(default_factory=dict)
    used: Dict[str, float] = field(default_factory=dict)
    total: Dict[str, float] = field(default_factory=dict)
    debt: Dict[str, float] = field(default_factory=dict)

    currencies: Dict[str, BalanceData] = field(default_factory=dict)


@dataclass
class MonitoringPoint:
    name: str
    value: Any
    group: str = None
    timestamp: float = None


class ChartPointDataType(Enum):
    FLOAT = "float"
    OHLCV = "ohlcv"


@dataclass
class ChartSchema:
    name: str
    data_type: ChartPointDataType
    chart_type: str = "line"
    chart_color: str = None
    y_axis: int = 0
    visible: bool = True

    def to_dict(self):
        data_dict = {}
        data_dict["name"] = self.name
        data_dict["data_type"] = self.data_type.value
        data_dict["chart_type"] = self.chart_type
        data_dict["y_axis"] = self.y_axis
        data_dict["chart_color"] = self.chart_color
        data_dict["visible"] = self.visible
        return data_dict


@dataclass
class ChartPoint:
    name: str
    value: Union[float, OHLCV]
    timestamp: int = 0


@dataclass
class AnnotationPoint:
    value: float
    message: str
    timestamp: int = 0


@dataclass
class LogMessagePoint:
    message: str
    stracktrace: str
    level: str
    timestamp: int = 0


class Position:
    def __init__(self, pair: str, position_type: PositionType):
        self.pair = pair
        self.position_type = position_type
        self.trades: List[Trade] = []
        self.balance: float = 0
        self.average_open: float = 0

    def _calculate_pnl(self, quantity: float, close_price: float):
        # Adjust PNL calculation for long and short positions
        if self.position_type == PositionType.LONG:
            pnl = (close_price - self.average_open) * quantity
        else:
            pnl = (self.average_open - close_price) * quantity
        return pnl

    def add_trade(self, trade: Trade):

        self.trades.append(trade)

        if trade.trade_type == TradeType.OPEN:
            self.update_average_open(self.balance, trade.quantity, trade.price)
            self.balance += trade.quantity

        else:
            if trade.quantity > self.balance:
                raise ValueError("Closing quantity exceeds current balance")
            self.balance -= trade.quantity

            pnl = self._calculate_pnl(trade.quantity, trade.price)

            trade.pnl = pnl

        if self.balance == 0:
            self.average_open = 0  # Reset average when position is fully closed

    def update_average_open(self, current_balance, new_quantity, new_price):
        if current_balance == 0:
            self.average_open = new_price
        else:
            total_value = current_balance * self.average_open + new_quantity * new_price
            new_total_quantity = current_balance + new_quantity
            self.average_open = total_value / new_total_quantity

    def get_trade(self, trade_id):
        for trade in self.trades:
            if trade.trade_id == trade_id:
                return trade
        return None  # Return None if no trade is found with the given trade_id

    def to_dict(self):
        data_dict = {}
        data_dict["pair"] = self.pair
        data_dict["position_type"] = self.position_type.value
        data_dict["balance"] = self.balance
        data_dict["average_open"] = self.average_open
        data_dict["pnl"] = self.total_pnl
        data_dict["timestamp"] = self.createdAt
        # data_dict["trades"] = [trade.to_dict() for trade in self.trades]

        return data_dict

    @property
    def createdAt(self):
        open_trades = [
            trade.timestamp
            for trade in self.trades
            if trade.trade_type == TradeType.OPEN
        ]
        if open_trades:
            return min(open_trades)
        return get_now_ts()

    @property
    def tradedQuantity(self):
        open_trades = [
            trade.quantity
            for trade in self.trades
            if trade.trade_type == TradeType.OPEN
        ]
        if open_trades:
            return sum(open_trades)
        return 0

    @property
    def total_pnl(self):
        return sum(
            trades.pnl for trades in self.trades if trades.trade_type == TradeType.CLOSE
        )

    def __repr__(self):
        return (
            f"Position(pair={self.pair}, type={self.position_type}, balance={self.balance}, "
            f"average_open={self.average_open}, trades={self.trades})"
        )


@dataclass
class TradeJournal:
    trade_timestamps: List[datetime]
    trade_pnls: List[float]
    account_balances: List[float]
    period_returns: List[float]
    total_returns: List[float]
    max_drawdown: float


@dataclass
class SessionStats:
    average_win: float
    average_loss: float
    num_winning_trades: int
    num_losing_trades: int
    winning_trades_pnl: float
    losing_trades_pnl: float
    win_streak: int
    loss_streak: int
    close_trade_count: int
    trade_journal: TradeJournal
