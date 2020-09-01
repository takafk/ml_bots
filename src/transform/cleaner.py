from prefect import Task
from typing import Dict, Any, Tuple
import datetime
import pandas as pd

__all__ = ["_to_safe_name", "clean_data_investing"]


def _to_safe_name(symbol: str):
    return symbol.replace("/", "")


def unix_to_datetime(x):
    return datetime.datetime.fromtimestamp(int(x[:-3]))


class Clean_Investing(Task):
    """Clean raw data.
    """

    def __init__(
        self, storage: str, serializer: Serializer = JSONSerializer, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.storage = storage
        self.serializer = serializer

    def raw(self, raw_data: Tuple[Dict[str, Any], Dict[str, str]]) -> tuple:

        raw = pd.read_json(raw_data[0])
        meta = raw_data[1]

        # Drop currency information
        if "Currency" in raw.columns:
            # Remove unnessary column
            raw.drop("Currency", inplace=True, axis=1)

        # Change name of columns
        if len(raw.columns) == 4:
            raw.columns = ["open", "high", "low", "close"]
        else:
            raw.columns = ["open", "high", "low", "close", "volume"]

        # To price from interest rate
        if meta["asset"] == "bond":
            # maturity is 10 year and we set base price as 100 for 1% rate.
            raw = 100 - (raw - 1) * 10

        return (raw, _to_safe_name(meta["symbol"]))
