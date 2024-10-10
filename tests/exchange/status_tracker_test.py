import pytest
from superalgorithm.exchange.status_tracker import (
    StatusTracker,
    update_mark_ts,
    get_latest_price,
)


@pytest.fixture
def tracker():
    return StatusTracker()


def test_update_ts_price(tracker):
    pair = "BTC/USD"
    timestamp = 1625097600
    mark_price = 34000.0

    update_mark_ts(pair, timestamp, mark_price, tracker=tracker)
    latest_price = get_latest_price(pair, tracker=tracker)

    assert latest_price.timestamp == timestamp
    assert latest_price.mark == mark_price


def test_update_ts_price_with_older_timestamp(tracker):

    pair = "BTC/USD"
    timestamp_new = 1625097600
    timestamp_old = 1625097500
    mark_price_new = 34000.0
    mark_price_old = 33000.0

    update_mark_ts(pair, timestamp_new, mark_price_new, tracker=tracker)
    update_mark_ts(pair, timestamp_old, mark_price_old, tracker=tracker)
    latest_price = get_latest_price(pair, tracker=tracker)

    assert latest_price.timestamp == timestamp_new
    assert latest_price.mark == mark_price_new


def test_update_ts_price_with_newer_timestamp(tracker):

    pair = "BTC/USD"
    timestamp_old = 1625097500
    timestamp_new = 1625097600
    mark_price_old = 33000.0
    mark_price_new = 34000.0

    update_mark_ts(pair, timestamp_old, mark_price_old, tracker)
    update_mark_ts(pair, timestamp_new, mark_price_new, tracker)
    latest_price = get_latest_price(pair, tracker=tracker)

    assert latest_price.timestamp == timestamp_new
    assert latest_price.mark == mark_price_new
