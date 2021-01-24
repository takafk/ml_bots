import pandas as pd
from dataclasses import dataclass, field

from core import ComputePipe
from .rawdata import RawData


@dataclass(frozen=True)
class LSRatioLO(ComputePipe):
    """Last observed long/short ratio.

    Notes:
        - Because the time interval of long short ratio is 5 min,
        we fill NaN with pervious observed value.
    """

    key: str = field(default="buy_ratio")

    def inputs(self, dsmeta):
        return RawData(key=self.key).compute(dsmeta)

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        lsratio = self.inputs(dfmeta)

        return lsratio.fillna(method="ffill", limit=5)
