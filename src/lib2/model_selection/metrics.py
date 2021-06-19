"""Metrics functions
"""
import pandas as pd
import numpy as np


def ic(y_true: pd.Series, y_pred: pd.Series):
    return np.corrcoef(y_true.values, y_pred.values)[0, 1]


def spread_return_sharpe_ratio(
    y_true: pd.Series, y_pred: pd.Series, q_class: int, cost: float
):

    metrics = {}

    q_class = min(10, len(set(y_true.index)))

    quantile_pred = y_pred.groupby(y_true.index).apply(
        lambda x: pd.cut(x, q_class, labels=False)
    )

    quantile_return = (
        y_true.groupby([y_true.index, quantile_pred]).mean().unstack()
    )

    long_return = quantile_return[q_class - 1] - cost
    short_return = -quantile_return[0] - cost
    spread_return = (long_return + short_return) / 2

    metrics["spread_sharpe"] = spread_return.mean() / spread_return.std()
    metrics["mean_spread_return"] = spread_return.mean()
    metrics["std_spread_return"] = spread_return.std()

    metrics["long_sharpe"] = long_return.mean() / long_return.std()
    metrics["short_sharpe"] = short_return.mean() / short_return.std()
    metrics["mean_long_return"] = long_return.mean()
    metrics["mean_short_return"] = short_return.mean()
    metrics["length"] = len(spread_return)

    return metrics


def compute_metrics(
    y_true: pd.Series, y_pred: pd.Series, q_class: int, cost: float
):

    metrics = spread_return_sharpe_ratio(y_true, y_pred, q_class, cost)
    metrics["ic"] = ic(y_true, y_pred)

    return metrics
