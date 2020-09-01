from typing import Dict, Any
from prefect import Task
import investpy

__all__ = [
    "Fetch_data_investing",
]


class Fetch_data_investing(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self, symbols: Dict[str, str]) -> Dict[str, Any]:

        assert symbols["asset"] in ["currency", "bond", "stock_index"]

        asset = symbols["asset"]
        symbol = symbols["symbol"]

        if asset == "currency":

            df = investpy.currency_crosses.get_currency_cross_historical_data(
                symbol,
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

        return df.to_json()
