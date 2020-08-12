import datetime
import time
import json
import requests

import pandas as pd
import numpy as np
from dataclasses import dataclass, field


@dataclass(frozen=True)
class GET_DATA:

    market: str = 'binance'
    chart_sec: int = field(default=60)
    data_path: str = field(default="./datastore/")
    name = field(default="./test.json")

    # Cryptowatchのデータを単に保存するだけの関数
    def _get_raw_data(self, market, crypto, min, path, before=0, after=0):

        # APIで価格データを取得
        params = {"periods": min}
        if before != 0:
            params["before"] = before
        if after != 0:
            params["after"] = after
        response = requests.get("https://api.cryptowat.ch/markets/{}/{}/ohlc".format(self.market, crypto), params)

        data = response.json()

        # ファイルに書き込む
        file = open(self.data_path, "w" ,encoding="utf-8")
        json.dump(data['result'], file)
        file.close()


    def _fix_columns_tojson(self, path, crypto):

        with open(file, 'r') as f:
            data = json.load(f)
            df = pd.DataFrame(data['60'])

        df.drop(6, axis=1, inplace=True)
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']

        df.index = df['timestamp']
        df.drop('timestamp', axis=1, inplace=True)

        df.to_json("".join([path, crypto, ".json"])

    # Accomodate datasets
    def store_data(self, crypots):

        for crypto in cryptos:

            self._get_raw_data(self.market, crypto, self.chart_sec, file ,after=10)

            self._fix_columns_tojson(DATA_PATH, crypto)


CRYPTOS = ['btcusdt', 'ethusdt', 'xrpusdt', 'eosusdt']


def main():

    get_data = GET_DATA()

    get.data.store_data(CRYPTOS)

if __name__ == '__main__':

    main()