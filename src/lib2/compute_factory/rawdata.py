from dataclasses import dataclass, field
import pandas as pd

from core import ComputePipe


@dataclass(frozen=True)
class RawData(ComputePipe):
    """Query rawdata from datastore."""

    key: str = field(default="close")

    def __post_init__(self):
        object.__setattr__(self, "name", f"RawData({str(self.key)})")

    def inputs(self, dsmeta: tuple):
        return None

    def compute(self, dsmeta: tuple) -> pd.DataFrame:

        datastore = dsmeta[0]
        symbols = dsmeta[1]

        # Query raw data from the datastore.
        result: dict = datastore.query_datas(keys=[self.key])

        # index = timestamp, columns = symbols
        df: pd.DataFrame = result[self.key]

        assert set(symbols) <= set(
            df.columns.tolist()
        ), "Several symbols do not exist in the datastore."

        return df[symbols]
