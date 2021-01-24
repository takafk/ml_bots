import pandas as pd
import investpy
from prefect.engine.result import Result
from prefect.engine.results import LocalResult

from core import DataTask

__all__ = [
    "FetchInvesting",
]


class FetchInvesting(DataTask):
    def __init__(self, result: Result = LocalResult(), *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.result = result
        self.target = "{symbols[symbol]}.pkl"

    def run(self, symbols) -> pd.DataFrame:

        symbol = symbols["symbol"]
        asset = symbols["asset"]
        country = symbols["country"]

        if asset == "currency":

            pair = "".join([symbol[:3], "/", symbol[3:]])

            df = investpy.currency_crosses.get_currency_cross_historical_data(
                pair,
                as_json=False,
                order="ascending",
                from_date="01/01/1960",
                to_date="09/09/2021",
            )

        elif asset == "bond":

            df = investpy.get_bond_historical_data(
                bond=symbol, from_date="01/01/1960", to_date="09/09/2021"
            )

        else:
            country = symbols["country"]

            df = investpy.get_index_historical_data(
                symbol, country, from_date="01/01/1960", to_date="09/09/2021"
            )

        return df
