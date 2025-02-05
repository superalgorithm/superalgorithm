from typing import Any
from superalgorithm.exchange.ccxt_exchange import CCXTExchange
from superalgorithm.types.data_types import Order


class WOOExchange(CCXTExchange):
    def __init__(self, params: Any = {}):
        super().__init__("woo", params)

        self.hedge_mode = params.get("hedge_mode", False)

    def _create_order_params(self, order: Order) -> dict:
        params = {
            "client_order_id": order.client_order_id,
        }
        if self.hedge_mode:
            params["position_side"] = order.position_type.value
        return params

    async def _create_limit_order(self, order: Order) -> str:
        return await super()._create_limit_order(
            order, self._create_order_params(order)
        )

    async def _create_market_order(self, order: Order) -> str:
        return await super()._create_market_order(
            order, self._create_order_params(order)
        )
