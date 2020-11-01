from typing import Tuple
import datetime
import pandas as pd
import json

from prefect.engine.results import LocalResult
from prefect.engine.result import Result
from core import DataTask

__all__ = ["_to_safe_name", "CleanDataInvesting", "CleanDataCryptowatch"]


def _to_safe_name(symbol: str):
    return symbol.replace("/", "")


def unix_to_datetime(x):
    return datetime.datetime.fromtimestamp(int(x[:-3]))


def unix_to_datetime_int(x):
    return datetime.datetime.fromtimestamp(int(x))


class CleanDataInvesting(DataTask):
    """Clean raw data from investing.
    """

    def __init__(self, result: Result = LocalResult(), *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.result = result
        self.target = "{raw_data[1][symbol]}.pkl"

    def run(self, raw_data: tuple) -> Tuple[pd.DataFrame, dict]:

        raw: pd.DataFrame = raw_data[0]
        meta: dict = raw_data[1]

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

        return raw


class CleanDataCryptowatch(DataTask):
    """Clean raw data from Cryptowatch.
    """

    def __init__(self, result: Result = LocalResult(), *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.result = result
        self.target = "{crypto_name}.pkl"

    def run(self, path: str, crypto_name: str) -> pd.DataFrame:

        with open(f"{path}/{crypto_name}.json", "r") as f:
            df = json.load(f)

        df = pd.DataFrame(df["result"]["60"])

        df = df.drop(columns=6)

        df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

        # To datetime data
        df["timestamp"] = df["timestamp"].apply(unix_to_datetime_int)

        df.index = df.timestamp

        df.drop(columns="timestamp", inplace=True)

        return df.reindex(
            pd.date_range(start=df.index[0], end=df.index[-1], freq="Min"), axis="index"
        )
