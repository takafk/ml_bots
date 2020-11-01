import pandas as pd
from dataclasses import dataclass, field

from core import Compute
from ta import add_all_ta_features

__all__ = [
    "All_Technicals",
    "AllTechnicals_WithVol",
]


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
        'volume_vpt',
    ]

    deny_cols = features.columns.isin(
        many_none + ["open", "high", "low", "close", "volume"] + leak_col
    )
    features = features.loc[:, ~deny_cols]

    return features.loc[:, features.columns[features.isnull().sum() >= max_nan]]


@ dataclass
class All_Technicals(Compute):

    window: int = field(default=63)
    quantile: float = field(default=0.01)
    dfmeta: tuple = field(default_factory=tuple)

    def inputs(self):
        return self.dfmeta

    def compute(self) -> pd.DataFrame:

        inputs = self.inputs()

        data: pd.DataFrame = inputs[0]

        # Dummy volume
        data["volume"] = 100

        # Add all ta features from ta module without leakage.
        features = add_all_ta_without_leak(
            data, open="open", high="high", low="low", close="close", volume="volume"
        )

        # Normalization and Treatment for extreme value
        normed_features = features.apply(
            lambda x: (x.rolling(self.window).mean() - x) /
            x.rolling(self.window).std()
        )
        normed_features = normed_features.apply(
            lambda x: x.clip(x.quantile(self.quantile),
                             x.quantile(1 - self.quantile))
        )

        return normed_features


@ dataclass
class AllTechnicals_WithVol(Compute):

    window: int = field(default=63)
    quantile: float = field(default=0.01)
    dfmeta: tuple = field(default_factory=tuple)
    normalize = True

    def inputs(self):
        return self.dfmeta

    def compute(self) -> pd.DataFrame:

        inputs = self.inputs()

        data: pd.DataFrame = inputs[0]

        # Add all ta features
        features = add_all_ta_without_leak(
            data, open="open", high="high", low="low", close="close", volume="volume"
        )

        if self.normalize:

            # Normalization and Treatment for extreme value
            # zscored_features = features.apply(lambda x: (x.rolling(self.window).mean() - x) / x.rolling(self.window).apply(lambda x: x.quantile(0.75) - x.quantile(0.25)))

            zscored_features = features.apply(
                lambda x: (x.mean() - x) /
                (x.quantile(0.75) - x.quantile(0.25))
            )
            features = zscored_features.apply(
                lambda x: x.clip(
                    x.quantile(self.quantile), x.quantile(1 - self.quantile)
                )
            )

        return features
