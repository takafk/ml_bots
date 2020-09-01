import datetime
import time
import json
import requests

import pandas as pd
import numpy as np
from dataclasses import dataclass, field


@dataclass(frozen=True)
class GET_DATA:

    market: str = field(default="binance")
    chart_sec: int = field(default=60)
    data_path: str = field(default="./datastore/")
    name: str = field(default="./data.json")

    # Cryptowatchのデータを単に保存するだけの関数
    def _get_raw_data(self, market, crypto, chart_sec, file, before=0, after=0):

        # APIで価格データを取得
        params = {"periods": chart_sec}
        if before != 0:
            params["before"] = before
        if after != 0:
            params["after"] = after
        response = requests.get(
            "https://api.cryptowat.ch/markets/{}/{}/ohlc".format(self.market, crypto),
            params,
        )

        data = response.json()

        # To dataframe
        df = pd.DataFrame(data["result"]["60"]).drop(6, axis=1)

        # Rename columns and index
        df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
        df.index = df["timestamp"]
        df.drop("timestamp", axis=1, inplace=True)

        # Accomodate data with json
        df.to_json("".join([self.data_path, crypto, ".json"]))

    def store_data(self, CRYPTOS):

        for crypto in CRYPTOS:

            self._get_raw_data(self.market, crypto, self.chart_sec, self.name, after=10)


CRYPTOS = ["btcusdt", "ethusdt", "xrpusdt", "eosusdt"]


def main():

    get_data = GET_DATA()

    get_data.store_data(CRYPTOS)


if __name__ == "__main__":
    main()
