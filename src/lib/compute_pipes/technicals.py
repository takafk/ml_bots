import pandas as pd
from dataclasses import dataclass, field

from core import Compute
from ta import add_all_ta_features

from .base import Open, Close, High, Low, Volume


def add_all_ta_without_leak(
    df: pd.DataFrame,
    open="open",
    high="high",
    low="low",
    close="close",
    volume="volume",
):

    max_nan = df.isnull().sum().max()

    features = add_all_ta_features(
        df, open="open", high="high", low="low", close="close", volume="volume"
    )

    # Remove unnessary and volume features
    many_none = [
        "trend_psar_up",
        "trend_psar_down",
        "trend_adx",
        "trend_adx_pos",
        "trend_adx_neg",
        "momentum_kama",
        "volatility_kcp",
        "volatility_atr",
        "trend_mass_index",
    ]

    leak_col = [
        "trend_kst",
        "trend_kst_sig",
        "trend_kst_diff",
        "trend_visual_ichimoku_a",
        "trend_visual_ichimoku_b",
        "others_dr",
        "volume_vpt",
    ]

    deny_cols = features.columns.isin(
        many_none + ["open", "high", "low", "close", "volume"] + leak_col
    )
    features = features.loc[:, ~deny_cols]

    return features.loc[:, features.columns[features.isnull().sum() >= max_nan]]


@dataclass(frozen=True)
class All_Technicals(Compute):

    window: int = field(default=63)
    quantile: float = field(default=0.01)

    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.DataFrame = inputs[0]

        # Dummy volume
        data["volume"] = 100

        # Add all ta features from ta module without leakage.
        features = add_all_ta_without_leak(
            data, open="open", high="high", low="low", close="close", volume="volume"
        )

        # Normalization and Treatment for extreme value
        normed_features = features.apply(
            lambda x: (x.rolling(self.window).mean() - x) / x.rolling(self.window).std()
        )
        normed_features = normed_features.apply(
            lambda x: x.clip(x.quantile(self.quantile), x.quantile(1 - self.quantile))
        )

        return normed_features


@dataclass(frozen=True)
class AllTechnicals_WithVol(Compute):

    window: int = field(default=63)
    quantile: float = field(default=0.01)
    normalize = True

    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.DataFrame = inputs[0]

        # Add all ta features
        features = add_all_ta_without_leak(
            data, open="open", high="high", low="low", close="close", volume="volume"
        )

        if self.normalize:

            # Normalization and Treatment for extreme value
            # zscored_features = features.apply(lambda x: (x.rolling(self.window).mean() - x) / x.rolling(self.window).apply(lambda x: x.quantile(0.75) - x.quantile(0.25)))

            zscored_features = features.apply(
                lambda x: (x.mean() - x) / (x.quantile(0.75) - x.quantile(0.25))
            )
            features = zscored_features.apply(
                lambda x: x.clip(
                    x.quantile(self.quantile), x.quantile(1 - self.quantile)
                )
            )

        return features


@dataclass(frozen=True)
class Return(Compute):
    """ Return of price.
    """

    window: int = field(default=60)
    base: Compute = field(default=Close())

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"Return({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dfmeta):
        return self.base.compute(dfmeta)

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.Series = inputs

        return data.pct_change(self.window)


@dataclass(frozen=True)
class MADiv(Compute):
    """ Moving Average Divergence.
    """

    window: int = field(default=60)
    base: Compute = field(default=Close())

    def __post_init__(self):
        object.__setattr__(self, "name", f"MADiv({str(self.base.name)}, {self.window})")

    def inputs(self, dfmeta):
        return self.base.compute(dfmeta)

    def compute(self, dfmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.Series = inputs

        mean = data.rolling(self.window, min_periods=1).mean()

        return (data - mean) / mean


@dataclass(frozen=True)
class Support(Compute):
    """ Supportive line.
    """

    window: int = field(default=60)
    base: Compute = field(default=Low())

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"Support({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dfmeta):
        return self.base.compute(dfmeta)

    def compute(self, dfmeta) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.Series = inputs

        support = data.rolling(self.window, min_periods=1).min()

        return (data - support) / support


@dataclass(frozen=True)
class Resistance(Compute):
    """ Resistance line.
    """

    window: int = field(default=60)
    base: Compute = field(default=High())

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"Resistance({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dfmeta):
        return self.base.compute(dfmeta)

    def compute(self, dfmeta) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.Series = inputs

        resistance = data.rolling(self.window, min_periods=1).max()

        return (data - resistance) / resistance


@dataclass(frozen=True)
class Volatility(Compute):
    """ Historical Volatility.
    """

    base: Compute = field(default=Return(window=1, base=Close()))
    window: int = field(default=60)

    def __post_init__(self):
        object.__setattr__(
            self, "name", f"Volatility({str(self.base.name)}, {self.window})"
        )

    def inputs(self, dfmeta):
        return self.base.compute(dfmeta)

    def compute(self, dfmeta) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        data: pd.Series = inputs

        vol = data.rolling(self.window, min_periods=1).std()

        return vol


@dataclass(frozen=True)
class VWAP(Compute):
    """ Volume　Weighted Average Price.
    """

    window: int = field(default=60)
    base: Compute = field(default=Close())

    def __post_init__(self):
        object.__setattr__(self, "name", f"VWAP({str(self.base.name)}, {self.window})")

    def inputs(self, dfmeta):
        return self.base.compute(dfmeta), Volume().compute(dfmeta)

    def compute(self, dfmeta) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        price: pd.Series = inputs[0]
        volume: pd.Series = inputs[1]

        trading_price = (price * volume).rolling(self.window, min_periods=1).sum()

        vwap = trading_price / volume.rolling(self.window, min_periods=1).sum()

        return (price - vwap) / vwap


@dataclass(frozen=True)
class Liquidity(Compute):
    """ Liquidity estimated by Open and Close.
    """

    def inputs(self, dfmeta):
        return Open().compute(dfmeta), Close().compute(dfmeta), Volume().compute(dfmeta)

    def compute(self, dfmeta) -> pd.DataFrame:

        inputs = self.inputs(dfmeta)

        open: pd.Series = inputs[0]
        close: pd.Series = inputs[1]
        volume: pd.Series = inputs[2]

        mid_price = (open + close) / 2

        return (open - close).abs() / (mid_price * volume)
