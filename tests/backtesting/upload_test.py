import pytest
from runmachine.backtesting.upload import upload_backtest
from runmachine.exchange.status_tracker import update_mark_ts
from runmachine.types.data_types import OHLCV, ChartPointDataType, ChartSchema
from runmachine.utils.logging import annotate, chart, set_chart_schema


def create_test_chart():
    set_chart_schema(
        [
            ChartSchema("series1", ChartPointDataType.OHLCV, "line", "red"),
            ChartSchema(
                "series2", ChartPointDataType.FLOAT, "scatter", "green", y_axis=1
            ),
        ]
    )

    # Add some values to the chart
    start_value = 1727350894162
    for i in range(10000):
        update_mark_ts("BTC/USD", start_value + i, 100)
        chart(
            name="series1",
            value=OHLCV(start_value + i, 100, 110, 90, 105, 1000),
        )
        chart(name="series2", value=10.0)

    # Annotate the chart
    annotate(
        value=105.0,
        message="High point",
    )
    annotate(
        value=10.0,
        message="Another point",
    )


@pytest.mark.asyncio
async def test_upload_backtest():
    create_test_chart()
    await upload_backtest()
