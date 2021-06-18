import pandas as pd
from dataclasses import dataclass

from core import ComputePipe

from .rawdata import RawData
from .technicals import Return


@dataclass(frozen=True)
class MarketReturn(ComputePipe):
    """Market Return (weighted average of trading value)"""

    def inputs(self, dsmeta):
        return (
            Return().compute(dsmeta),
            RawData(key="volume").compute(dsmeta),
            RawData(key="close").compute(dsmeta),
        )

    def compute(self, dsmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        returns = inputs[0]
        volume = inputs[1]
        close = inputs[2]

        trading_value = volume.mul(close)
        weights = trading_value.div(trading_value.sum(axis=1), axis="index")

        return returns.mul(weights, fill_value=None).sum(axis=1)
