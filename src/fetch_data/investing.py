from typing import List, Dict, Any
import investpy

__all__ = [
    "investing_fx",
    "investing_stock_indices",
    "investing_bond_10y",
]


def investing_fx(path: str, base: str, currencies: List[str]):
    """ Fetch historical data of forex from investing.com.

    Args:
        path (str): path for saving json file.
        base (str): name of base currency. e.g. 'JPY'.
        currencies (List[str]): List of currencies.
    """

    for currency in currencies:
        df_currency = investpy.currency_crosses.get_currency_cross_historical_data(
            currency,
            as_json=False,
            order="ascending",
            from_date="01/01/1960",
            to_date="09/09/2021",
        )

        df_currency.to_json(path + base + currency[4:] + ".json")


def investing_stock_indices(path: str, countries: List[str], symbols: Dict[str, Any]):
    """ Fetch historical data of stock indices from investing.com

    Args:
        path (str): path for saving json file.
        countries (List[str]): list of countries which we focus on.
        indices (Dict[str]): dictionary of symbols about stock indices by country,
        e.g. {'japan':['N 225']}
    """

    for country in countries:

        symbols = symbols[country]

        indices_list = investpy.indices.get_indices(country=country)

        for symbol in symbols:

            name = indices_list.loc[indices_list["symbol"]
                                    == symbol, "name"].values[0]

            df_index = investpy.get_index_historical_data(
                name, country, from_date="01/01/1960", to_date="09/09/2021"
            )

            df_index.to_json(path + symbol.replace(" ", "_") + ".json")


def investing_bond_10y(path: str, countries: List[str]):
    """Fetch historical data of bonds from investing.com

    Args:
        path (str): path for saving json file.
        bonds (List[str]): list of bond names.
    """

    bonds = []

    for country in countries:

        bond_names = investpy.bonds.get_bonds(country=country)

        bonds.append(
            bond_names.loc[bond_names.name.str.contains('10'), 'name'].values[0])

    for bond in bonds:
        df_bond = investpy.get_bond_historical_data(
            bond=bond, from_date='01/01/1960', to_date='09/09/2021')

        df_bond.to_json(path + bond.replace(' ', '') + ".json")
