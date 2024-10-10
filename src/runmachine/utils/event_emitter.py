from abc import ABC
import asyncio
from typing import Any, Callable, Dict, List, TypeVar, Generic

T = TypeVar("T", bound=Callable[..., None])


class EventEmitter(ABC, Generic[T]):
    def __init__(self):
        self.listeners: Dict[str, List[T]] = {}

    def dispatch(self, event: str, *args: Any, **kwargs: Any) -> None:
        """
        Dispatch an event to all listeners. Async listeners are dispatched as tasks.
        """
        if event in self.listeners:
            for listener in self.listeners[event]:
                if asyncio.iscoroutinefunction(listener):
                    asyncio.create_task(listener(*args, **kwargs))
                else:
                    listener(*args, **kwargs)

    async def dispatch_and_await(self, event, *args, **kwargs):
        """
        Dispatch an event and await all listeners to complete.
        """
        if event in self.listeners:
            for listener in self.listeners[event]:
                if asyncio.iscoroutinefunction(listener):
                    await listener(*args, **kwargs)
                else:
                    listener(*args, **kwargs)

    def on(self, event: str, listener: T) -> None:
        if event not in self.listeners:
            self.listeners[event] = []
        self.listeners[event].append(listener)

        return self
