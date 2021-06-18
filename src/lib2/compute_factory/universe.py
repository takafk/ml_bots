import pandas as pd
from dataclasses import dataclass, field
from .technicals import Liquidity

from core import ComputePipe


@dataclass(frozen=True)
class LIQUIDITYTOP(ComputePipe):

    window: int = field(default=480)
    top_k: int = field(default=20)
    liquidity: ComputePipe = field(default=Liquidity())

    def inputs(self, dsmeta):
        return self.liquidity.compute(dsmeta)

    def compute(self, dsmeta) -> pd.DataFrame:

        liquidity = self.inputs(dsmeta)

        mean_liq = liquidity.rolling(self.window).mean()

        rank = mean_liq.rank(axis=1, ascending=False)

        symbols = rank.columns[(rank <= self.top_k).iloc[-1]].tolist()

        assert len(symbols) == self.top_k

        return symbols
