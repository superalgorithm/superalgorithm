import pytest

from superalgorithm.data.providers.csv import CSVDataSource


@pytest.mark.asyncio
async def test_csv_import():
    csv_datasource = CSVDataSource("BTC/USDT", "5m", aggregations=["1h"])

    await csv_datasource.connect()

    data = []
    async for item in csv_datasource.read():
        data.append(item)

    assert len(data) > 0, "Expected non-empty data from CSVDataSource.read()"
