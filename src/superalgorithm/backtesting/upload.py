from superalgorithm.utils.api_client import api_call
from superalgorithm.utils.logging import log_exception
from superalgorithm.utils.logging import strategy_monitor


async def upload_backtest(initial_cash: int = 10000):
    """
    Uploads the backtest data to the server for further analysis.
    The initial cash should be the same as what was used for the paper exchange instance.
    """
    try:
        data = strategy_monitor.serialize()
        data["config"] = {"initial_cash": initial_cash}
        api_call("v1-backtest", json=data)
    except Exception as e:
        log_exception(e, "Uploading backtest failed")
