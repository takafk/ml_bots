import pandas as pd
import numpy as np

from ta import add_all_ta_features
from ta.utils import dropna

from sklearn.preprocessing import LabelEncoder

JST = timezone(timedelta(hours=+9), "JST")


def unix_to_datetime(x):
    return datetime.datetime.fromtimestamp(int(x), JST)


def load_latest_data(path, cryptos):

    dict_crypto = {}

    for crypto in cryptos:

        with open("".join([path, crypto, ".json"]), "r") as f:

            tmp = pd.DataFrame(json.load(f))

            tmp.index = tmp.index.to_series().apply(unix_to_datetime)

            dict_crypto[crypto] = tmp

    return dict_crypto


@dataclass(frozen=True)
class Dataset:

    dict_crypto: dict
    window: int = 10
    cost: float = 0.00075

    def _create_labels(self, dict_crypto, window, cost):

        labels = pd.DataFrame()

        for crypto in dict_crypto.keys():

            if len(labels) == 0:
                labels = dict_crypto[crypto].close.rename(crypto)
            else:
                labels = pd.merge(
                    left=labels,
                    right=dict_crypto[crypto].close.rename(crypto),
                    left_index=True,
                    right_index=True,
                    how="outer",
                )

        # Continuous future retrun
        label_num = labels.pct_change(window).shift(-window).stack()

        ranges = [-20, -cost, cost, 20]

        # Categorical future retrun
        label_cat, bins = pd.cut(label_num, ranges, labels=False, retbins=True)
        class_num = len(bins) - 1

        return (label_cat, class_num)

    def _create_features(self, dict_crypto):

        features = []

        # Accomodate datasets
        for crypto in dict_crypto.keys():

            # Clean NaN values
            tmp = dropna(dict_crypto[crypto])

            # Add all ta features
            tmp = add_all_ta_features(
                tmp, open="open", high="high", low="low", close="close", volume="volume"
            )

            many_none = ["trend_psar_up", "trend_psar_down"]
            tmp = tmp.loc[
                :,
                ~(
                    tmp.columns.isin(
                        many_none + ["open", "high", "low", "close", "volume"]
                    )
                ),
            ]

            tmp["symbol"] = crypto

            # To multindex
            tmp.index = [tmp.index, tmp["symbol"]]

            features.append(tmp)

        features = pd.concat(features, sort=False)
        features = features.dropna().sort_index(level=0)

        # Categorical feature for symbol
        le = LabelEncoder()
        le = le.fit(features["symbol"].values)
        features["symbol"] = le.transform(features["symbol"].values)

        return features

    def _feature_selection(datasets):

        lowest_ic_features = list(datasets.corr()["label"].sort_values()[:10].index)
        largest_ic_features = list(datasets.corr()["label"].sort_values()[-10:].index)

        selected_features = list(
            set(lowest_ic_features + largest_ic_features + ["symbol"])
        )

        datasets = datasets[selected_features]

        return datasets

    def create_dataset(self):

        labels_cat, class_num = self._create_labels(
            self.dict_crypto, self.window, self.cost
        )

        features = self._create_features(self.dict_crypto)

        datasets = pd.DataFrame(labels_cat.rename("label").copy())
        datasets[features.columns] = features
        datasets = datasets.dropna()

        datasets_selected = _feature_selection(datasets)

        return dataset
