import asyncio
from collections import defaultdict
from dataclasses import asdict
import warnings
import traceback
from typing import Any, Dict, List, Literal, Optional, Union

from superalgorithm.types.data_types import (
    OHLCV,
    AnnotationPoint,
    ChartPoint,
    ChartPointDataType,
    ChartSchema,
    LogMessagePoint,
    MonitoringPoint,
    Order,
    Trade,
    Position,
)
from superalgorithm.utils.api_client import upload_log
from superalgorithm.utils.helpers import guid
from superalgorithm.utils.event_emitter import EventEmitter
from superalgorithm.utils.helpers import get_now_ts
from superalgorithm.utils.config import config


class StrategyMonitor(EventEmitter):

    _monitoring: Dict[str, MonitoringPoint] = {}
    _chart_schema: Dict[str, ChartSchema] = {}
    _chart_points: List[ChartPoint] = []
    _annotations: List[AnnotationPoint] = []
    _log: List[LogMessagePoint] = []
    _trades: List[Trade] = []
    _orders: List[Order] = []
    _positions: List[Position] = []

    def __init__(self, upload_interval: int = 10):
        super().__init__()

        self.initialized = True
        self.upload_interval = upload_interval
        self.session_id = guid()
        self.strategy_id = config.get("SUPER_STRATEGY_ID")
        self._task = None

    def set_upload_interval(self, upload_interval):
        self.upload_interval = upload_interval

    def start(self):
        """
        Start the periodic upload process, this is called during live trading sessions.
        """
        if (
            config.get("SUPER_STRATEGY_ID") is None
            or config.get("SUPER_API_KEY") is None
            or config.get("SUPER_API_SECRET") is None
            or config.get("SUPER_API_ENDPOINT") is None
        ):
            warnings.warn(
                "SUPER_API_KEY, SUPER_API_SECRET, SUPER_API_ENDPOINT are required to enable live monitoring."
            )
            return

        self._task = asyncio.create_task(self._upload_periodically())
        return self._task

    def stop(self):
        """
        Stop the periodic upload process gracefully.
        """
        if self._task:
            self._task.cancel()
            self._task = None

    def add_data_point(self, data_point: Any, timestamp=None):
        # Order, Trade, ChartPoint and AnnotationPoint use the highest timestamp seen by the strategy to support backtesting.
        from superalgorithm.exchange.status_tracker import get_highest_timestamp

        if isinstance(data_point, MonitoringPoint):
            data_point.timestamp = get_now_ts() if timestamp is None else timestamp
            self._monitoring[data_point.name] = data_point
        elif isinstance(data_point, ChartPoint):
            data_point.timestamp = (
                get_highest_timestamp() if timestamp is None else timestamp
            )
            self._chart_points.append(data_point)
        elif isinstance(data_point, AnnotationPoint):
            data_point.timestamp = (
                get_highest_timestamp() if timestamp is None else timestamp
            )
            self._annotations.append(data_point)
        elif isinstance(data_point, LogMessagePoint):
            data_point.timestamp = get_now_ts() if timestamp is None else timestamp
            self._log.append(data_point)
        elif isinstance(data_point, Trade):
            data_point.timestamp = (
                get_highest_timestamp() if timestamp is None else timestamp
            )
            self._trades.append(data_point)
        elif isinstance(data_point, Order):
            data_point.timestamp = (
                get_highest_timestamp() if timestamp is None else timestamp
            )
            self._orders.append(data_point)
        elif isinstance(data_point, Position):
            data_point.timestamp = (
                get_highest_timestamp() if timestamp is None else timestamp
            )
            self._positions.append(data_point)

    async def _upload_periodically(self):
        while True:
            await asyncio.sleep(self.upload_interval)
            self._upload_data()

    def convert_chart_data(self, chart_points: List[ChartPoint]):
        """
        Convert the chart data into a chart chunk that can be uploaded to the server.
        """

        grouped_chart_points = defaultdict(list)
        unique_timestamps = set()

        # Group ChartPoints by name and collect unique timestamps
        for chart_point in chart_points:
            grouped_chart_points[chart_point.name].append(chart_point)
            unique_timestamps.add(chart_point.timestamp)

        sorted_timestamps = sorted(unique_timestamps)  # Sort unique timestamps

        data_array_settings = {
            schema.name: {
                "length": (1 if schema.data_type == ChartPointDataType.FLOAT else 5),
                "data_index": sum(
                    1 if s.data_type == ChartPointDataType.FLOAT else 5
                    for s in list(self._chart_schema.values())[:i]
                ),
            }
            for i, schema in enumerate(self._chart_schema.values())
        }

        data_array_length = sum(v["length"] for v in data_array_settings.values())
        # Initialize the data dictionary with a 0s filled array as per the required length for each timestamp
        # i.e. a schema with 2 float fields and 1 OHLCV field will have a length of 7 and is initialized as [0, 0, 0, 0, 0, 0, 0]
        data = {timestamp: [0] * data_array_length for timestamp in sorted_timestamps}

        # Populate the data with actual values from the chart points
        for schema_name, schema in self._chart_schema.items():
            # Get index offset based on the schema order
            index_offset = data_array_settings[schema_name]["data_index"]
            # Get points for this chart name and populate the result arrays
            if schema_name in grouped_chart_points:
                for point in grouped_chart_points[schema_name]:
                    timestamp = point.timestamp
                    if schema.data_type == ChartPointDataType.FLOAT:
                        data[timestamp][index_offset] = point.value
                    elif schema.data_type == ChartPointDataType.OHLCV:
                        data[timestamp][index_offset : index_offset + 5] = [
                            point.value.open,
                            point.value.high,
                            point.value.low,
                            point.value.close,
                            point.value.volume,
                        ]

        return {
            "timestamp": sorted_timestamps[0],
            "schema": {
                key: {
                    **s.to_dict(),
                    "data_index": data_array_settings[s.name]["data_index"],
                }
                for key, s in self._chart_schema.items()
            },
            "data": data,
        }

    def serialize(self):
        """
        Returns a flat list of dicts with {'type': 'monitoring', 'name': 'alex', 'val... etc. as per the fields in the data point.
        """
        data = []
        if len(self._chart_points) > 0:
            data.extend(
                [
                    {
                        "type": "chart",
                        "value": self.convert_chart_data(self._chart_points),
                    }
                ]
            )
        data.extend(
            [{"type": "annotation", "value": {**asdict(a)}} for a in self._annotations]
        )
        data.extend([{"type": "log", "value": {**asdict(e)}} for e in self._log])
        data.extend(
            [
                {"type": "variable", "value": {**asdict(m)}}
                for m in self._monitoring.values()
            ]
        )
        data.extend([{"type": "trade", "value": {**t.to_dict()}} for t in self._trades])
        data.extend([{"type": "order", "value": {**o.to_dict()}} for o in self._orders])
        data.extend(
            [{"type": "position", "value": {**p.to_dict()}} for p in self._positions]
        )

        return {
            "strategy_id": self.strategy_id,
            "session_id": self.session_id,
            "updates": data,
        }

    def clear(self):
        self._monitoring.clear()
        self._chart_points.clear()
        self._annotations.clear()
        self._log.clear()
        self._trades.clear()
        self._orders.clear()
        self._positions.clear()

    def _upload_data(self):
        try:
            data_for_upload = self.serialize()
            self.clear()
            # TODO: we can retry on error to upload list_to_upload, but we have to clear the buffer here

            if len(data_for_upload.get("updates")) > 0:
                print("Uploading log data", data_for_upload)
                upload_log(data_for_upload)
                self.dispatch("upload_complete")
                return

            print("Nothing to upload. Skipping upload.")
        except Exception as e:
            from superalgorithm.utils.logging import log_exception

            log_exception(e, "Uploading log failed")


strategy_monitor = StrategyMonitor()


# User-facing functions
def monitor(name, value: Any, group=None, timestamp=None, stdout=True):
    strategy_monitor.add_data_point(
        MonitoringPoint(name=name, value=value, group=group), timestamp=timestamp
    )
    if stdout:
        print(name, value)


def set_chart_schema(schemas: List[ChartSchema]):
    strategy_monitor._chart_schema = {schema.name: schema for schema in schemas}


def chart(name: str, value: Union[float, OHLCV], timestamp=None):
    schema = strategy_monitor._chart_schema.get(name)
    if schema is None:
        raise ValueError(
            f"Chart schema for {name} is not set. Use set_chart_schema first."
        )
    elif isinstance(value, OHLCV) and schema.data_type != ChartPointDataType.OHLCV:
        raise ValueError(
            f"Chart schema for {name} is not OHLCV. Use chart with float value."
        )
    elif isinstance(value, float) and schema.data_type != ChartPointDataType.FLOAT:
        raise ValueError(
            f"Chart schema for {name} is not float. Use chart with float value."
        )

    if value is None:
        value = 0

    strategy_monitor.add_data_point(ChartPoint(name=name, value=value), timestamp)


def annotate(value: Any, message: str, timestamp=None):
    AnnotationPoint(value=value, message=message)
    strategy_monitor.add_data_point(
        AnnotationPoint(value=value, message=message), timestamp
    )


def log_exception(e: Exception, message: str = "", timestamp=None, stdout=True):

    strategy_monitor.add_data_point(
        LogMessagePoint(
            message=f"{message} - {str(e)}",
            stracktrace=traceback.format_exc(),
            level="ERROR",
        ),
        timestamp=timestamp,
    )
    if stdout:
        print(f"{message} - {str(e)}")


def log_message(
    message,
    level: Optional[Literal["ERROR", "WARN", "INFO", "DEBUG"]] = "INFO",
    timestamp=None,
    stdout=True,
):
    strategy_monitor.add_data_point(
        LogMessagePoint(message=message, stracktrace="", level=level), timestamp
    )
    if level == "WARN":
        warnings.warn(message)
    elif stdout:
        print(message)


def log_trade(trade: Trade, timestamp=None, stdout=True):
    strategy_monitor.add_data_point(trade, timestamp)
    if stdout:
        print(trade)


def log_order(order: Order, timestamp=None, stdout=True):
    strategy_monitor.add_data_point(order, timestamp)
    if stdout:
        print(order)


def log_position(position: Position, timestamp=None, stdout=True):
    strategy_monitor.add_data_point(position, timestamp)
    if stdout:
        print(position)
