from typing import Optional
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from sklearn import linear_model
from joblib import Parallel, delayed

from core import ComputePipe

from .benchmark import MarketReturn
from .technicals import Return


@dataclass(frozen=True)
class Beta(ComputePipe):
    """EWM Beta (coefficient to MarketReturn)"""

    window: int = field(default=120)

    def __post_init__(self):
        object.__setattr__(self, "name", f"Beta({self.window})")

    def inputs(self, dsmeta):
        return (MarketReturn().compute(dsmeta), Return().compute(dsmeta))

    def compute(self, dsmeta: tuple) -> pd.DataFrame:

        inputs = self.inputs(dsmeta)

        benchmark = inputs[0]
        returns = inputs[1]

        returns["benchmark"] = benchmark.reindex(returns.index)

        jobs = []

        for i, day in enumerate(returns[self.window - 1 :].index):

            df = returns.iloc[i : i + self.window]

            jobs.append(delayed(self._compute_beta_bysymbol)(df.dropna()))

            output = Parallel(n_jobs=4, verbose=1)(jobs)

        betas = pd.DataFrame([o[1] for o in output], index=[o[0] for o in output])
        betas.colums = returns.columns

        return betas

    def _robustregress_coeff(
        self,
        X: np.array,
        y: np.array,
        weights: Optional[np.array] = None,
    ):

        clf = linear_model.HuberRegressor()

        clf.fit(X, y, sample_weight=weights)

        return clf.coef_

    def _compute_weights(self, window: int):

        raw_weights = np.exp(np.arange(window) / window)

        return raw_weights / raw_weights.sum()

    def _zscore(self, x):
        return x - x.mean() / x.std()

    def _compute_beta_bysymbol(self, df: pd.DataFrame):

        index = df.index[-1]
        length = len(df)

        weights = self._compute_weights(length)

        betas = []

        for col in df.columns:
            if col != "benchmark":
                X = df.loc[:, "benchmark"].values.reshape(-1, 1)
                y = df.loc[:, col].values.reshape(-1, 1)

                beta = self._robustregress_coeff(X=X, y=y, weights=weights)
                betas.append(beta[0])

        return (index, betas)
