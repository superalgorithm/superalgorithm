from abc import ABC
import asyncio
from typing import Any, Callable, Coroutine, Dict, List, TypeVar, Generic

# T = TypeVar("T", bound=Callable[..., None])
EventHandler = Callable[..., None | Coroutine]


class EventEmitter(ABC):
    def __init__(self):
        super().__init__()
        self.listeners: Dict[str, List[EventHandler]] = {}

    def dispatch(self, event: str, *args: Any, **kwargs: Any) -> None:
        """
        Dispatch an event to all listeners. Async listener functions are dispatched as tasks and run asynchronously, while sync functions are run synchronously.
        """
        if event in self.listeners:
            for listener in self.listeners[event]:
                if asyncio.iscoroutinefunction(listener):
                    asyncio.create_task(listener(*args, **kwargs))
                else:
                    listener(*args, **kwargs)

    async def dispatch_and_await(self, event, *args, **kwargs):
        """
        Dispatch an event to all listeners but only return when async listener functions are done.
        """
        if event in self.listeners:
            for listener in self.listeners[event]:
                if asyncio.iscoroutinefunction(listener):
                    await listener(*args, **kwargs)
                else:
                    listener(*args, **kwargs)

    def on(self, event: str, listener: EventHandler) -> None:
        if event not in self.listeners:
            self.listeners[event] = []
        self.listeners[event].append(listener)

        return self
