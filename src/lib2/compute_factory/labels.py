import pandas as pd
from dataclasses import dataclass, field

from core import ComputePipe
from .rawdata import RawData

__all__ = [
    "FwdReturn",
]


@dataclass(frozen=True)
class FwdReturn(ComputePipe):

    window: int = field(default=10)
    base: ComputePipe = field(default=RawData(key="close"))

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"(FwdReturn{str(self.base.name)}, {self.window})"
        )

    def inputs(self, dsmeta):
        return self.base.compute(dsmeta)

    def compute(self, dsmeta) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        data: pd.Series = inputs

        OFF_SET = 1  # we can have position from the next day
        fwd_return = data.pct_change(self.window).shift(-self.window - OFF_SET)

        return fwd_return
