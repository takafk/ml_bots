from typing import Any, List, Optional
from dataclasses import dataclass, field
import pandas as pd
import h5py

from core import ComputePipe


@dataclass(frozen=True)
class RawData(ComputePipe):
    """Query rawdata from datastore."""

    key: str = field(default="close")

    def __post_init__(self):
        object.__setattr__(self, "name", f"RawData({str(self.key)})")

    def inputs(self, dsmap: tuple):
        return None

    def compute(self, dsmap: tuple) -> pd.DataFrame:

        datastore = dsmap[0]
        symbols = dsmap[1]

        with h5py.File(datastore, "r") as f:
            assert self.key in list(
                f.keys()
            ), "Given key does not exist in the datastore."

        # index = timestamp, columns = symbols
        df: pd.DataFrame = pd.read_hdf(datastore, key=self.key)

        assert set(symbols) <= set(
            df.columns.tolist()
        ), "Several symbols do not exist in the datastore."

        return df[symbols]
