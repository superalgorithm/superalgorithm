import os
import csv
import csv
from typing import List
from superalgorithm.types import OHLCV


def append_to_csv(file_path, data):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(data)


def load_historical_data(
    pair: str, timeframe: str, csv_data_folder: str = ".history", offset_msec=0
) -> List[OHLCV]:
    """
    Load historical data for a given trading pair and timeframe, applying an optional offset to the timestamps.

    Requires a 'csv_data_folder' config parameter to be set, with the path where you store your CSV files.
    The CSV files must be named {pair}_{timeframe}.csv, where / will be replaced with _
    Example: load_historical_data("BTC/USDT", "5m") will load from {csv_data_folder}/BTC_USDT_5m.csv

    Args:
        pair (str): Trading pair, e.g., 'BTC/USDT'.
        timeframe (str): Timeframe, e.g., '1d', '1h', '1m'.
        offset_msec (int, optional): Offset in milliseconds to adjust timestamps. Default is 0.

        When to use offset_msec (and a better alternative explained below):

            (Note: this issue only applies to backtesting!)

            Imagine you have two CSV files with daily and hourly bars.
            For the daily dataset each bar represents an entire day, but the timestamp is usually set to the start of the day (00:00).
            However, the final closing price for that day isn't fully confirmed until the end of the day (23:59).

            If you are now using both hourly and daily bars from two CSVDataSources, and you do something like hourly.close > daily.close you would get false results (because daily.close is in the future at (23:59)).

            To avoid this issue, you could add an offset to the daily bars. For example, adding a 24-hour delay (86400000) means the engine would only send the bar by end of the day.

            BUT THERE IS A MUCH SIMPLER ALTERNATIVE:
            Use the aggregations property of the CSVDataSource to work with multiple timeframes. Only load the smallest timeframe you need and use 'aggregations' to compute the higher timeframes.

    Returns:
        List[OHLCV]: list of OHLCV values
    """

    with open(
        f'{csv_data_folder}/{pair.replace("/", "_")}_{timeframe}.csv',
        newline="",
    ) as csvfile:
        reader = csv.reader(csvfile)
        data = [
            OHLCV(
                int(row[0]) + offset_msec,
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
            )
            for row in reader
        ]

    return data
