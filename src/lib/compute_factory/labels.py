import pandas as pd
from dataclasses import dataclass, field

from core import Compute
from .base import Close

__all__ = [
    "FwdReturn",
]


@dataclass(frozen=True)
class FwdReturn(Compute):

    window: int = field(default=10)
    base = Close()

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"(FwdReturn{str(self.base.name)}, {self.window})"
        )

    def inputs(self, dfmeta):
        return self.base.compute(dfmeta)

    def compute(self, dfmeta) -> pd.Series:

        inputs = self.inputs(dfmeta)

        data: pd.Series = inputs

        OFF_SET = 1  # we can have position from the next day
        fwd_return = data.pct_change(self.window).shift(-self.window - OFF_SET)

        return fwd_return
