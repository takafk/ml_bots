from dataclasses import dataclass
from IPython.display import display, Markdown

import pandas as pd
import matplotlib.pyplot as plt


@dataclass
class AlphaReport:

    dataset: pd.DataFrame
    window: int

    def compute_quantile_return(self, bins: int = 4):

        score_quantile = (
            self.dataset["prediction"]
            .groupby(pd.Grouper(level=0))
            .apply(lambda x: pd.qcut(x, bins, labels=False, duplicates="drop"))
        )

        quantile_return = (
            self.dataset["label"]
            .groupby([pd.Grouper(level=0), score_quantile])
            .mean()
            .unstack()
        )

        resampled = quantile_return.resample(f"{self.window}min").mean()

        return resampled

    def compute_drawdown(self, cum_returns: pd.Series):
        max = cum_returns.cummax()
        return (cum_returns - max) / (1 + max)

    def compute_daily_metrics(self, returns: pd.DataFrame):

        metrics = pd.DataFrame(
            returns.groupby(returns.index.day)
            .count()
            .rename("Number of Trade")
        )
        metrics["Return(%)"] = (
            returns.groupby(returns.index.day).sum().rename("Daily Return(%)")
            * 100
        ).round(3)
        metrics["Max DrawDown(%)"] = (
            returns.groupby(returns.index.day).apply(
                lambda x: self.compute_drawdown(x.cumsum()).min()
            )
            * 100
        )
        metrics["Return per Trade(%)"] = (
            metrics["Return(%)"] / metrics["Number of Trade"]
        )
        metrics["Sharpe Ratio"] = returns.groupby(returns.index.day).apply(
            lambda x: x.mean() / x.std()
        )
        metrics.loc["Avg.", :] = metrics.mean()

        return metrics

    def plot_spread_return(self, cost=0.05 / 100):

        quantile_return = self.compute_quantile_return()

        long = (
            quantile_return[quantile_return.columns.max()].rename("long")
            - cost
        )

        short = (
            -quantile_return[quantile_return.columns.min()].rename("short")
            - cost
        )

        spread = (long + short).rename("spread") / 2
        drawdown = self.compute_drawdown(spread.cumsum())

        display(Markdown("# Spread Return Sheet"))
        fig, axes = plt.subplots(3, 1, figsize=(24, 24))
        quantile_return.cumsum().plot(ax=axes[0], grid=True)
        axes[0].set_title("Quantile Return")
        spread.cumsum().plot(ax=axes[1], grid=True)
        axes[1].set_title("Spread Return")
        axes[2] = drawdown.plot.area()
        axes[2].set_title("DrawDown")

        plt.grid()
        plt.legend()
        plt.show()
        plt.close()

        display(Markdown("## Daily Metrics"))
        daily_metrics = self.compute_daily_metrics(spread)
        display(daily_metrics.T)

        display(Markdown("## Return per Trade"))
        spread.hist(bins=50)
        display(spread.describe())
        plt.show()
        plt.close()

    def display_tearsheet(self):

        display(Markdown("# No Cost"))
        self.plot_quantile_return(cost=0)

        display(Markdown("# Cost 0.05%"))
        self.plot_quantile_return(cost=0.05 / 100)
