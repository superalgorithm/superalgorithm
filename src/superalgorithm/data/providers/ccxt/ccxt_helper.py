import ccxt
import time
from typing import List, Optional
from ccxt.base.errors import NetworkError, ExchangeError
from superalgorithm.data.providers.csv.csv_helper import append_to_csv
from superalgorithm.types import OHLCV
from superalgorithm.utils.config import config
from superalgorithm.utils.helpers import get_now_ts
from superalgorithm.utils.logging import log_exception, log_message


def fetch_ohlcv(
    symbol: str,
    timeframe: str,
    start_ts: int,
    exchange_id: str = "binance",
    save_to_csv: bool = False,
    sleep_time: int = 2,
    limit: int = 1000,
) -> List[OHLCV]:
    """
    Fetch OHLCV (Open, High, Low, Close, Volume) data for a given cryptocurrency symbol and return List[OHLCV].

    Args:
        symbol (str): The trading pair (e.g., 'BTC/USDT').
        timeframe (str): The time interval for the candlesticks.
        start_ts (int): The starting timestamp for fetching data.
        exchange_id (str, optional): The exchange to use. Defaults to "binance".
        save_to_csv (bool, optional): Flag to save data to a CSV file. Defaults to False.
        sleep_time (int, optional): Time to sleep between API calls in seconds. Defaults to 2.
        limit (int, optional): Number of candles to fetch per API call. Defaults to 1000.

    Returns:
        List[OHLCV]: A list of OHLCV objects containing the fetched data.

    Raises:
        ValueError: If input parameters are invalid.
        NetworkError: If there's a network-related error.
        ExchangeError: If there's an exchange-specific error.
    """
    # Input validation
    if not all([symbol, timeframe, start_ts]):
        raise ValueError("symbol, timeframe, and start_ts are required parameters")

    if save_to_csv and config.get("CSV_DATA_FOLDER") is not None:
        raise ValueError(
            "CSV_DATA_FOLDER is set, but saving to CSV is also enabled. This is not allowed."
        )

    try:
        exchange = getattr(ccxt, exchange_id)()
    except AttributeError:
        raise ValueError(f"Invalid exchange_id: {exchange_id}")

    end_ts = get_now_ts()
    data: List[OHLCV] = []

    def load(since: int) -> Optional[List[OHLCV]]:
        try:
            kline = exchange.fetch_ohlcv(
                symbol=symbol, timeframe=timeframe, since=since, limit=limit
            )
            if kline:
                if save_to_csv:
                    append_to_csv(
                        f'{config.get("CSV_DATA_FOLDER")}/{symbol.replace("/", "_")}_{timeframe}.csv',
                        kline,
                    )

                data.extend([OHLCV(*candle) for candle in kline])

                next_ts = (
                    kline[-1][0] + 60000
                )  # add one minute TODO: should work as the smallest resolution is one minute, so this offset will push it to the next candle even for higher timeframes

                if next_ts < end_ts:
                    log_message(
                        f"Fetching more data. Progress: {(next_ts - start_ts) / (end_ts - start_ts) * 100:.2f}%"
                    )
                    time.sleep(sleep_time)
                    return load(next_ts)

            return data
        except NetworkError as e:
            log_exception(
                e, f"Network error occurred: Retrying in {sleep_time} seconds..."
            )
            time.sleep(sleep_time)
            return load(since)
        except ExchangeError as e:
            log_exception(e, f"Exchange error occurred: Stopping data fetch.")
            return None

    result = load(start_ts)
    if result:
        result.sort(key=lambda x: x.timestamp)
        return result
    else:
        return []
