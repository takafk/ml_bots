import pandas as pd
from dataclasses import dataclass

from core import Compute


@dataclass(frozen=True)
class Open(Compute):
    """open price."""

    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.DataFrame = inputs[0]

        return data["open"]


@dataclass(frozen=True)
class Close(Compute):
    """close price."""

    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.DataFrame = inputs[0]

        return data["close"]


@dataclass(frozen=True)
class High(Compute):
    """high price."""

    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.DataFrame = inputs[0]

        return data["high"]


@dataclass(frozen=True)
class Low(Compute):
    """low price."""

    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.DataFrame = inputs[0]

        return data["low"]


@dataclass(frozen=True)
class Volume(Compute):
    """volume."""

    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.DataFrame = inputs[0]

        return data["volume"]


@dataclass(frozen=True)
class All(Compute):
    """All OHLCV data.

    Note:
        - This is for All_Technicals.
    """

    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.DataFrame = inputs[0]

        return data
