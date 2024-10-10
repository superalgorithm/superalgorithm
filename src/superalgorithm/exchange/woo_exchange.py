from typing import Any
from superalgorithm.exchange.ccxt_exchange import CCXTExchange
from superalgorithm.types.data_types import Order


class WOOExchange(CCXTExchange):

    def __init__(self, config: Any = {}):
        super().__init__("woo", config)

    async def _create_limit_order(self, order: Order) -> str:
        return await super()._create_limit_order(
            order,
            {
                "client_order_id": order.client_order_id,
                "position_side": order.position_type.value,
            },
        )

    async def _create_market_order(self, order: Order) -> str:
        return await super()._create_market_order(
            order,
            {
                "client_order_id": order.client_order_id,
                "position_side": order.position_type.value,
            },
        )
