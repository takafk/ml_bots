from typing import List
from dataclasses import dataclass
import pandas as pd
import h5py


@dataclass(frozen=True)
class LocalDataStore:

    path: str

    def list_symbols(self):

        with h5py.File(self.path, "r") as f:
            key = list(f.keys())[0]

        df: pd.DataFrame = pd.read_hdf(self.path, key=key)

        return df.columns.tolist()

    def query_datas(self, keys: List[str]) -> dict:

        with h5py.File(self.path, "r") as f:

            for key in keys:

                assert key in list(
                    f.keys()
                ), "Given key does not exist in the datastore."

        result = {}

        # index = timestamp, columns = symbols
        for key in keys:
            result[key] = pd.read_hdf(self.path, key=key)

        return result
