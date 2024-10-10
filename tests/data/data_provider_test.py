import pytest
from unittest.mock import Mock
from runmachine.data.data_provider import DataProvider
from runmachine.data.providers.ccxt.ccxt_data_source import CCXTDataSource
from runmachine.utils.helpers import get_lookback_timestamp


@pytest.mark.asyncio
async def test_preload_live_switch():
    """
    It's the data providers responsibility to switch from preload to live mode when the preload data is exhausted.
    """

    preload_data_mock = Mock()
    live_data_mock = Mock()
    preload_ts = get_lookback_timestamp(days=1)

    # NOTE: starting mode has to be PRELOAD or PAPER. The strategy set's the initial mode based on the exchange instance. Since we are not using a strategy for this test we have to manually set it.

    datasource = CCXTDataSource(
        "BTC/USDT",
        "5m",
        exchange_id="binance",
        preload_from_ts=preload_ts,
    )

    data_provider = DataProvider()
    data_provider.add_data_source(datasource)

    async for bar, is_live in data_provider.stream_data():
        if not is_live:
            preload_data_mock(bar)
        if is_live:
            live_data_mock(bar)
            break

    assert preload_data_mock.call_count > 0, "Expected at least one preload data update"
    assert live_data_mock.call_count > 0, "Expected at least one live data update"
