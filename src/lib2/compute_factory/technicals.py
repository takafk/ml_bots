import pandas as pd
from dataclasses import dataclass, field

from core import ComputePipe

from .rawdata import RawData


@dataclass(frozen=True)
class Return(ComputePipe):
    """Return of price."""

    window: int = field(default=1)
    base: ComputePipe = field(default=RawData(key="close"))

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"Return({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dsmeta):
        return self.base.compute(dsmeta)

    def compute(self, dsmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        data: pd.DataFrame = inputs

        return data.pct_change(self.window, fill_method=None)


@dataclass(frozen=True)
class MADiv(ComputePipe):
    """Moving Average Divergence."""

    window: int = field(default=60)
    base: ComputePipe = field(default=Return())

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"MADiv({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dsmeta):
        return self.base.compute(dsmeta)

    def compute(self, dsmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        data: pd.Series = inputs

        mean = data.rolling(self.window, min_periods=1).mean()

        return (data - mean) / mean


@dataclass(frozen=True)
class Support(ComputePipe):
    """Supportive line."""

    window: int = field(default=60)
    base: ComputePipe = field(default=RawData(key="low"))

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"Support({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dsmeta):
        return self.base.compute(dsmeta)

    def compute(self, dsmeta) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        data: pd.Series = inputs

        support = data.rolling(self.window, min_periods=1).min()

        return (data - support) / support


@dataclass(frozen=True)
class Resistance(ComputePipe):
    """Resistance line."""

    window: int = field(default=60)
    base: ComputePipe = field(default=RawData(key="high"))

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"Resistance({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dsmeta):
        return self.base.compute(dsmeta)

    def compute(self, dsmeta) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        data: pd.Series = inputs

        resistance = data.rolling(self.window, min_periods=1).max()

        return (data - resistance) / resistance


@dataclass(frozen=True)
class Volatility(ComputePipe):
    """Historical Volatility."""

    base: ComputePipe = field(default=Return())
    window: int = field(default=60)

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"Volatility({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dsmeta):
        return self.base.compute(dsmeta)

    def compute(self, dsmeta) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        data: pd.Series = inputs

        vol = data.rolling(self.window, min_periods=1).std()

        return vol


@dataclass(frozen=True)
class VWAP(ComputePipe):
    """Volume　Weighted Average Price."""

    window: int = field(default=60)
    base: ComputePipe = field(default=RawData(key="close"))

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"VWAP({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dsmeta):
        return self.base.compute(dsmeta), RawData("volume").compute(dsmeta)

    def compute(self, dsmeta) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        price: pd.Series = inputs[0]
        volume: pd.Series = inputs[1]

        trading_price = (
            (price * volume).rolling(self.window, min_periods=1).sum()
        )

        vwap = trading_price / volume.rolling(self.window, min_periods=1).sum()

        return (price - vwap) / vwap


@dataclass(frozen=True)
class Liquidity(ComputePipe):
    """Amihud like Liquidity estimated by Open and Close.

    Args:
        open_price (ComputePipe): [description]
        close_price (ComputePipe): [description]
        volume (ComputePipe): [description]

    Returns:
        [type]: [description]

    Notes:
        - We change denominator not to have 0 while keeping the meaning of liquidity.
    """

    open_price: ComputePipe = field(default=RawData(key="open"))
    close_price: ComputePipe = field(default=RawData(key="close"))
    volume: ComputePipe = field(default=RawData(key="volume"))

    def inputs(self, dsmeta):
        return (
            self.open_price.compute(dsmeta),
            self.close_price.compute(dsmeta),
            self.volume.compute(dsmeta),
        )

    def compute(self, dsmeta) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        open_price: pd.Series = inputs[0]
        close_price: pd.Series = inputs[1]
        volume: pd.Series = inputs[2]

        returns = (open_price - close_price) / open_price
        mid_price = (open_price + close_price) * 0.5
        trading_value = mid_price * volume

        return trading_value / (1 + returns.abs())
