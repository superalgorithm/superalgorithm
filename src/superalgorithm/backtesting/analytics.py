from typing import List
from superalgorithm.types.data_types import SessionStats, Trade, TradeJournal, TradeType


def trade_journal(starting_balance: float, closed_trades: List[Trade]) -> TradeJournal:
    balance = starting_balance
    trade_pnls = [trade.pnl for trade in closed_trades]
    trade_timestamps = [trade.timestamp for trade in closed_trades]
    account_balances = [starting_balance]
    period_returns = [0]  # Start with 0% return
    drawdowns = []
    peak_balance = starting_balance

    # Calculate account growth and drawdown
    for pnl in trade_pnls:
        balance += pnl
        account_balances.append(balance)

        # Calculate cumulative return based on the updated balance
        if len(account_balances) > 1:
            previous_balance = account_balances[-2]
            period_returns.append((balance - previous_balance) / previous_balance)

        # Update peak balance and calculate drawdown
        if balance > peak_balance:
            peak_balance = balance
        drawdown = (peak_balance - balance) / peak_balance
        drawdowns.append(drawdown)

    # Calculate percentage returns relative to the starting balance
    total_returns = [
        (bal - starting_balance) / starting_balance for bal in account_balances
    ]

    # Get the maximum drawdown
    max_drawdown = max(drawdowns) if drawdowns else 0

    return TradeJournal(
        trade_timestamps=(
            [trade_timestamps[0]] + trade_timestamps if trade_timestamps else [0]
        ),
        trade_pnls=[0] + trade_pnls,
        account_balances=account_balances,
        period_returns=period_returns,
        total_returns=total_returns,
        max_drawdown=max_drawdown,
    )


def session_stats(trades: List[Trade]) -> SessionStats:
    initial_cash = 1000
    win_streak = 0
    loss_streak = 0
    current_win_streak = 0
    current_loss_streak = 0
    close_trade_count = 0
    losses = []
    wins = []

    for trade in trades:
        if trade.trade_type == TradeType.CLOSE:
            close_trade_count += 1
            if trade.pnl > 0:
                wins.append(trade.pnl)
                current_win_streak += 1
                current_loss_streak = 0
            else:
                losses.append(trade.pnl)
                current_loss_streak += 1
                current_win_streak = 0

            if current_win_streak > win_streak:
                win_streak = current_win_streak
            if current_loss_streak > loss_streak:
                loss_streak = current_loss_streak

    winning_trades_pnl = sum(wins)
    losing_trades_pnl = sum(losses)
    average_win = winning_trades_pnl / len(wins) if wins else 0
    average_loss = losing_trades_pnl / len(losses) if losses else 0
    closed_trades = [trade for trade in trades if trade.trade_type == TradeType.CLOSE]

    journal = trade_journal(initial_cash, closed_trades)

    return SessionStats(
        average_win=average_win,
        average_loss=average_loss,
        num_winning_trades=len(wins),
        num_losing_trades=len(losses),
        winning_trades_pnl=winning_trades_pnl,
        losing_trades_pnl=losing_trades_pnl,
        win_streak=win_streak,
        loss_streak=loss_streak,
        close_trade_count=close_trade_count,
        trade_journal=journal,
    )
