import pytest

from superalgorithm.data.providers.ccxt import CCXTDataSource
from superalgorithm.data.providers.ccxt import fetch_ohlcv
from superalgorithm.utils.helpers import get_lookback_timestamp


@pytest.mark.asyncio
async def test_ccxt_datasource():

    preload_ts = get_lookback_timestamp(days=1)

    datasource = CCXTDataSource(
        "BTC/USDT",
        "5m",
        aggregations=["1h"],
        exchange_id="woo",
        preload_from_ts=preload_ts,
    )

    await datasource.connect()

    updates = []
    async for item in datasource.read():
        updates.append(item)
        # there are 288 5 minute candles in a day, plus 2 for some live data will test we preloaded a day and got 2 live updates
        if len(updates) >= 290:
            break

    assert len(updates) >= 290, "Expected at 288 preload and 2 live updates"

    await datasource.disconnect()


def test_fetch_ohlcv():
    since = get_lookback_timestamp(hours=2000)
    results = fetch_ohlcv(
        "BTC/USDT", "1h", since, exchange_id="binance", save_to_csv=False
    )

    assert len(results) == 2000, "Expected 2000 candles"
