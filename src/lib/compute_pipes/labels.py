import pandas as pd
from dataclasses import dataclass, field

from core import Compute

__all__ = [
    "FwdReturn",
]


@dataclass
class FwdReturn(Compute):

    window: int = field(default=10)
    dfmeta: tuple = field(default_factory=tuple)

    def inputs(self):
        return self.dfmeta

    def compute(self) -> pd.DataFrame:

        inputs = self.inputs()

        ohlcdata: pd.DataFrame = inputs[0]

        OFF_SET = 1  # we can have position from the next day
        fwd_return = (
            ohlcdata["close"]
            .dropna()
            .pct_change(self.window)
            .shift(-self.window - OFF_SET)
        )

        return fwd_return.rename("fwdreturn({window})".format(window=self.window))
