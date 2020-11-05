import pandas as pd
from dataclasses import dataclass, field

from core import Compute


@dataclass
class MeanLSDiff(Compute):

    window: int = field(default=180)
    dfmeta: tuple = field(default_factory=tuple)

    def __post_init__(self):
        self.name = f"MeanLSDiff({self.window})"

    def inputs(self):
        return self.dfmeta

    def compute(self) -> pd.DataFrame:

        inputs = self.inputs()

        data: pd.DataFrame = inputs[0]

        return (
            (data["buy_ratio"] - data["sell_ratio"])
            .rolling(self.window, min_periods=1)
            .mean()
            .rename(self.name)
        )


@dataclass
class MeanLSRatio(Compute):

    window: int = field(default=180)
    dfmeta: tuple = field(default_factory=tuple)

    def __post_init__(self):
        self.name = f"MeanLSRatio({self.window})"

    def inputs(self):
        return self.dfmeta

    def compute(self) -> pd.DataFrame:

        inputs = self.inputs()

        data: pd.DataFrame = inputs[0]

        return (
            (data["buy_ratio"] / data["sell_ratio"])
            .rolling(self.window, min_periods=1)
            .mean()
            .rename(self.name)
        )


@dataclass
class MeanHLLSDiff(Compute):

    window: int = field(default=180)
    dfmeta: tuple = field(default_factory=tuple)

    def __post_init__(self):
        self.name = f"MeanHLLSDiff({self.window})"

    def inputs(self):
        return self.dfmeta

    def compute(self) -> pd.DataFrame:

        inputs = self.inputs()

        data: pd.DataFrame = inputs[0]

        hl_lsdiff = (data["buy_ratio"] - data["sell_ratio"]) * (
            data["high"] - data["low"]
        )

        return hl_lsdiff.rolling(self.window, min_periods=1).mean().rename(self.name)


@dataclass
class MeanOCLSDiff(Compute):

    window: int = field(default=180)
    dfmeta: tuple = field(default_factory=tuple)

    def __post_init__(self):
        self.name = f"MeanOCLSDiff({self.window})"

    def inputs(self):
        return self.dfmeta

    def compute(self) -> pd.DataFrame:

        inputs = self.inputs()

        data: pd.DataFrame = inputs[0]

        oc_lsdiff = (data["buy_ratio"] - data["sell_ratio"]) * (
            data["open"] - data["close"]
        )

        return oc_lsdiff.rolling(self.window, min_periods=1).mean().rename(self.name)
