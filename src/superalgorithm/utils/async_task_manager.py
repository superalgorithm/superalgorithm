import asyncio
from contextlib import suppress
from typing import List, Callable, Coroutine, Any

from superalgorithm.utils.event_emitter import EventEmitter


class AsyncTaskManager:
    def __init__(self):
        """
        Initialize the AsyncTaskManager with a list to store registered tasks.
        """
        self._registered_tasks: List[Callable[[], Coroutine[Any, Any, Any]]] = []
        self._active_tasks: List[asyncio.Task] = []

    def register_task(self, task_func: Callable[[], Coroutine[Any, Any, Any]]):
        """
        Register an async task function to be run when start() is called.

        :param task_func: A callable that returns a coroutine
        """
        self._registered_tasks.append(task_func)

    def unregister_task(self, task_func: Callable[[], Coroutine[Any, Any, Any]]):
        """
        Remove a previously registered task function.

        :param task_func: The task function to remove
        """
        if task_func in self._registered_tasks:
            self._registered_tasks.remove(task_func)

    async def start(self):
        """
        Start all registered tasks as coroutines.

        :return: asyncio.gather result of all tasks
        """

        self._active_tasks = [
            asyncio.create_task(task_func()) for task_func in self._registered_tasks
        ]

    async def stop(self):
        """
        Stop all running tasks gracefully.
        """

        # Cancel all active tasks
        for task in self._active_tasks:
            task.cancel()

        print("Cancelling tasks")
        # Wait for tasks to be cancelled
        await asyncio.gather(*self._active_tasks, return_exceptions=True)
        print("Cancelled")
        # Clear the active tasks list
        self._active_tasks.clear()

    def is_running(self) -> bool:
        """
        Check if any tasks are currently running.

        :return: Boolean indicating if tasks are active
        """
        return len(self._active_tasks) > 0
