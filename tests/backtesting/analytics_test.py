import pytest
from runmachine.backtesting.analytics import session_stats
from runmachine.types import PositionType
from runmachine.types.data_types import Trade, TradeType
from runmachine.utils.helpers import get_now_ts


@pytest.mark.asyncio
async def test_backtest_results():

    closed_trades = [
        Trade(
            trade_id=1,
            timestamp=get_now_ts(),
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.CLOSE,
            price=54000,
            quantity=1,
            server_order_id=123,
            pnl=50,
        ),
        Trade(
            trade_id=1,
            timestamp=get_now_ts(),
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.CLOSE,
            price=54000,
            quantity=1,
            server_order_id=123,
            pnl=75,
        ),
        Trade(
            trade_id=1,
            timestamp=get_now_ts(),
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.CLOSE,
            price=54000,
            quantity=1,
            server_order_id=123,
            pnl=-30,
        ),
        Trade(
            trade_id=1,
            timestamp=get_now_ts(),
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.CLOSE,
            price=54000,
            quantity=1,
            server_order_id=123,
            pnl=60,
        ),
        Trade(
            trade_id=1,
            timestamp=get_now_ts(),
            pair="BTC/USDT",
            position_type=PositionType.LONG,
            trade_type=TradeType.CLOSE,
            price=54000,
            quantity=1,
            server_order_id=123,
            pnl=40,
        ),
    ]

    results = session_stats(closed_trades)

    assert results is not None, "results should not be None"
    assert results.average_win == 56.25, "average win should be 56.25"
    assert results.average_loss == -30, "average loss should be -30"
    assert results.num_winning_trades == 4, "number of winning trades should be 4"
    assert results.num_losing_trades == 1, "number of losing trades should be 1"
    assert results.winning_trades_pnl == 225, "winning trades PNL should be 225"
    assert results.losing_trades_pnl == -30, "losing trades PNL should be -30"
    assert results.win_streak == 2, "win streak should be 2"
    assert results.loss_streak == 1, "loss streak should be 1"
    assert results.close_trade_count == 5, "close trade count should be 5"
    assert results.trade_journal.trade_pnls == [
        0,
        50,
        75,
        -30,
        60,
        40,
    ], "trade PnLs should match"
    assert results.trade_journal.account_balances == [
        1000,
        1050,
        1125,
        1095,
        1155,
        1195,
    ], "account balances should match"
    assert results.trade_journal.period_returns == [
        0,
        0.05,
        0.07142857142857142,
        -0.02666666666666667,
        0.0547945205479452,
        0.03463203463203463,
    ], "trade to trade returns should match"
    assert results.trade_journal.total_returns == [
        0,
        0.05,
        0.125,
        0.095,
        0.155,
        0.195,
    ], "total returns should match"
    assert (
        results.trade_journal.max_drawdown == 0.02666666666666667
    ), "max drawdown should be 2.666666666666667"

    assert (
        len(results.trade_journal.trade_timestamps)
        == len(results.trade_journal.trade_pnls)
        and len(results.trade_journal.trade_pnls)
        == len(results.trade_journal.account_balances)
        and len(results.trade_journal.account_balances)
        == len(results.trade_journal.total_returns)
        and len(results.trade_journal.total_returns)
        == len(results.trade_journal.period_returns)
    ), "lengths of trade journal lists should match"
