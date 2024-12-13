import random
import threading
import time


class UniqueIntGenerator:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._last_id = 0
        return cls._instance

    def generate_client_id(self) -> int:
        with self._lock:
            current_time = int(time.time() * 1000)
            random_component = random.randint(0, 9999)

            # Ensure monotonically increasing
            candidate_id = int(f"{current_time}{random_component:04d}")
            while candidate_id <= self._last_id:
                random_component = random.randint(0, 9999)
                candidate_id = int(f"{current_time}{random_component:04d}")

            self._last_id = candidate_id
            return candidate_id


# Singleton instance
unique_int_generator = UniqueIntGenerator()
