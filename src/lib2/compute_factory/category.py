from dataclasses import dataclass
import pandas as pd

from core import ComputePipe
from .rawdata import RawData


@dataclass(frozen=True)
class Symbol(ComputePipe):
    def inputs(self, dsmeta):
        return RawData(key="close").compute(dsmeta)

    def compute(self, dfmeta) -> pd.Series:

        close = self.inputs(dfmeta)

        for col in close.columns:

            close.loc[:, col] = col

        return close
