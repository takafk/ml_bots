import pandas as pd
from dataclasses import dataclass, field

from core import ComputePipe
from .rawdata import RawData
from .benchmark import MarketReturn

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

        price: pd.Series = inputs

        OFF_SET = 1  # we can have position from the next day
        fwd_return = price.pct_change(self.window, fill_method=None).shift(
            -self.window - OFF_SET
        )

        return fwd_return


@dataclass(frozen=True)
class DeMean(ComputePipe):

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

        price: pd.Series = inputs

        OFF_SET = 1  # we can have position from the next day
        fwd_return = (
            price.pct_change(self.window, fill_method=None)
            .shift(-self.window - OFF_SET)
            .groupby(pd.Grouper(level=0))
            .apply(lambda x: x - x.mean())
        )

        return fwd_return


@dataclass(frozen=True)
class DeMarket(ComputePipe):

    window: int = field(default=10)
    market_return: ComputePipe = field(default=MarketReturn())

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"(DeMarket{str(self.base.name)}, {self.window})"
        )

    def inputs(self, dsmeta):
        return (
            FwdReturn(window=self.window).compute(dsmeta),
            self.market_return.compute(dsmeta),
        )

    def compute(self, dsmeta) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        fwd_return: pd.Series = inputs[0]
        market_return: pd.Series = inputs[1]

        OFF_SET = 1  # we can have position from the next day

        mreturn = (
            market_return.rolling(self.window)
            .apply(lambda x: x.add(1).prod() - 1)
            .shift(-self.window - OFF_SET)
        )

        # One day de market return.
        de_market = fwd_return.sub(mreturn.reindex(fwd_return.index), axis="index")

        return de_market
