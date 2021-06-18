from typing import Tuple
import datetime
import pandas as pd
import json

from prefect.engine.results import LocalResult
from prefect.engine.result import Result
from core import DataTask


def unix_to_datetime(x):
    return datetime.datetime.fromtimestamp(int(x[:-3]))


def unix_to_datetime_int(x):
    return datetime.datetime.fromtimestamp(int(x), datetime.timezone.utc)


class CleanDataInvesting(DataTask):
    """Clean raw data from investing."""

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
    """Clean raw data from Cryptowatch."""

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

        # Convert unix to datetime with timezone (UTC)
        # We check this by comparing times on dataframe and my laptop time(JAPAN).
        df.index = df["timestamp"].apply(unix_to_datetime_int)

        df.drop(columns="timestamp", inplace=True)

        # Treatment for NaN in the middle of timestamp
        return df.reindex(
            pd.date_range(start=df.index[0], end=df.index[-1], freq="Min"), axis="index"
        )


class CleanBybitLSRatio(DataTask):
    """Clean raw data of long short ratio from Bybit."""

    def __init__(self, result: Result = LocalResult(), *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.result = result
        self.target = "{crypto_name}_lsratio.pkl"

    def run(self, path: str, crypto_name: str) -> pd.DataFrame:

        crypto_name = crypto_name.upper()

        with open(f"{path}/{crypto_name}_lsratio.json", "r") as f:
            df = json.load(f)

        df = pd.DataFrame(df)

        # Convert unix to datetime with timezone (UTC)
        # We check this by comparing times on dataframe and my laptop time(JAPAN).
        df.index = df.timestamp.apply(unix_to_datetime_int)

        df.drop(columns="timestamp", inplace=True)

        # Reindex to have complete index.
        # Basically we missed several datapoints in collecting data...
        df = df.reindex(pd.date_range(df.index[0], df.index[-1], freq="1min"))

        return df


class CleanBybitOHLC(DataTask):
    """Clean raw ohlc data of long short ratio from Bybit."""

    def __init__(self, result: Result = LocalResult(), *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.result = result
        self.target = "{crypto_name}_ohlc.pkl"

    def run(self, path: str, crypto_name: str) -> pd.DataFrame:

        crypto_name = crypto_name.upper()

        with open(f"{path}/{crypto_name}_ohlc.json", "r") as f:
            df = json.load(f)

        df = pd.DataFrame(df)[["open", "high", "low", "close", "volume", "open_time"]]

        # Convert unix to datetime with timezone (UTC)
        # We check this by comparing times on dataframe and my laptop time(JAPAN).
        df.index = df["open_time"].apply(unix_to_datetime_int)
        df.index.name = "timestamp"

        df.drop(columns="open_time", inplace=True)

        # Reindex to have complete index.
        # Basically we missed several datapoints in collecting data...
        df = df.reindex(pd.date_range(df.index[0], df.index[-1], freq="1min"))

        return df


class CleanBinanceOHLC(DataTask):
    """Clean raw ohlc data of long short ratio from Binance."""

    def __init__(self, result: Result = LocalResult(), *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.result = result
        self.target = "{crypto_name}_ohlc.pkl"

    def run(self, path: str, crypto_name: str) -> pd.DataFrame:

        crypto_name = crypto_name.upper()

        with open(f"{path}/{crypto_name}_ohlc.json", "r") as f:
            df = json.load(f)

        cols = [
            "open time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close time",
            "quote asset volume",
            "number of trades",
            "taker buy base asset volume",
            "taker buy quote asset volume",
            "ignore.",
        ]

        df = pd.DataFrame(df)
        df.columns = cols

        # Remove data without close time.
        df = df[~df["close time"].isnull()]

        # Convert unix to datetime with timezone (UTC)
        # We check this by comparing times on dataframe and my laptop time(JAPAN).
        df.index = (
            df["close time"]
            .apply(lambda x: int(float(format(x, "f"))))
            .apply(lambda x: datetime.datetime.fromtimestamp(int(str(x)[:-3])))
        )
        df.index.name = "timestamp"

        # Remove unnecessary columns
        df = df.loc[
            :,
            ~df.columns.isin(
                [
                    "open time",
                    "close time",
                    "quote asset volume",
                    "taker buy quote asset volume",
                    "ignore.",
                ]
            ),
        ]

        # Change data type from str to float.
        df[df.columns[df.dtypes == "object"]] = df[
            df.columns[df.dtypes == "object"]
        ].apply(lambda x: x.astype(float))

        # Remove duplications. This would occur in the time when Binance cannot record proper data
        # due to too intensive liquidity.
        df = df[~df.duplicated(keep=False)]

        # Reindex to have complete index.
        # Basically we missed several datapoints in collecting data...
        df = df.reindex(pd.date_range(df.index[0], df.index[-1], freq="1min"))

        return df
