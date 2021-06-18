import pandas as pd
from dataclasses import dataclass

from core import ComputePipe

from .rawdata import RawData
from .benchmark import MarketReturn


@dataclass(frozen=True)
class Extend(ComputePipe):
    """Extend pd.Series to pd.DataFrame."""

    series: ComputePipe = MarketReturn()
    base: ComputePipe = RawData(key="close")

    def __post_init__(self):
        object.__setattr__(
            self,
            "name",
            f"Extend(series={str(self.series.name)}, base={self.base.name})",
        )

    def inputs(self, dsmeta):
        return (
            self.series.compute(dsmeta),
            self.base.compute(dsmeta),
        )

    def compute(self, dsmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        series: pd.Series = inputs[0]
        base: pd.DataFrame = inputs[1]

        for col in base.columns:
            base.loc[:, col] = series.reindex(base.index)

        return base
