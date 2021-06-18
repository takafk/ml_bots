import pandas as pd
from dataclasses import dataclass

from core import ComputePipe
from .last_observed import LSRatioLO


@dataclass(frozen=True)
class LSRatio(ComputePipe):
    def inputs(self, dsmeta):
        return (
            LSRatioLO(key="buy_ratio").compute(dsmeta),
            LSRatioLO(key="sell_ratio").compute(dsmeta),
        )

    def compute(self, dsmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        long_ratio = inputs[0]
        sell_ratio = inputs[1]

        return long_ratio / sell_ratio
