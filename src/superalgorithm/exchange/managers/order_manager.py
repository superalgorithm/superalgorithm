import asyncio
from typing import Dict
from superalgorithm.types.data_types import Order, OrderStatus, Trade
from superalgorithm.utils.logging import log_message, log_order
from superalgorithm.exchange.base_exchange import BaseExchange


class OrderManager:
    def __init__(self, exchange: BaseExchange):

        self.exchange = exchange
        self.orders: Dict[str, Order] = {}

    def add_order(self, order: Order):
        if order.client_order_id in self.orders:
            raise ValueError(
                f"Order with client_order_id {order.client_order_id} already exists"
            )

        self.orders[order.client_order_id] = order
        log_order(order, stdout=False)

    def on_order_update(
        self, client_order_id: int, filled: float, order_status: OrderStatus
    ):
        order = self.orders[client_order_id]
        order.filled = filled

        if order.order_status != order_status:
            order.order_status = order_status
            order.dispatch(order.order_status.value, order)

    def get_order_by_server_id(self, server_order_id: str):
        for order in self.orders.values():
            if order.server_order_id == server_order_id:
                return order
        return None  # Return None if no order with the given ID is found

    def get_orders_by_status(self, order_status: OrderStatus):
        return [
            order
            for order in list(self.orders.values())
            if order.order_status == order_status
        ]

    def get_order_by_client_id(self, client_order_id: int):
        return self.orders.get(client_order_id)

    async def dispatch_deferred(self, client_order_id: int, order_status: OrderStatus):
        """
        Dispatches an order status update to the order with the given client_order_id, but allows other async tasks to perform in between.
        This is used to counter race conditions where we create an order, the order is filled instantly, but before we could add an event listener for the order status update.

        Example:
        order = await exchange.create_order(...) -> order is filled and closed, but we have not yet added an event listener
        order.add_event_listener(...) -> this will never trigger as it already happened
        """
        await asyncio.sleep(0)
        if client_order_id in self.orders:
            self.orders[client_order_id].dispatch(
                order_status, self.orders[client_order_id]
            )
