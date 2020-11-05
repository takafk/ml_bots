import pandas as pd
import pickle

from prefect.engine.results import LocalResult
from prefect.engine.result import Result
from core import DataTask


class DataSetBybit(DataTask):
    """ Compsite dataset for bybit.
    """

    def __init__(self, result: Result = LocalResult(), *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.result = result
        self.target = "{crypto_name}.pkl"

    def run(self, path: str, path_ls: str, crypto_name: str) -> pd.DataFrame:

        with open(f"{path}/{crypto_name}.pkl", "rb") as f:
            df = pickle.load(f)

        with open(f"{path_ls}/{crypto_name}_lsratio.pkl", "rb") as f:
            df_ls = pickle.load(f)

        df = df.merge(df_ls, right_index=True, left_index=True, how="outer")

        return df
