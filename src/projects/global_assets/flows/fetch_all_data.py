from typing import List
from prefect import Flow, Parameter
from prefect.engine.serializers import JSONSerializer
from prefect.engine.result import Result
from prefect.engine.results import LocalResult

from extract import Fetch_data_investing
from transform import _to_safe_name

__all__ = ["fetch_data_global"]


def fetch_data_global(
    symbols: List[dict], result: Result = LocalResult(serializer=JSONSerializer())
) -> Flow:
    """ Fetch all data for global assets from investing.com.

    Args:
        symbols (List[dict]): List of dictionary. each dictionary have 'symbol', 'asset', 'country' key.
        result (Result): Result instance to write data.
    """

    def _get_symbol(**kwargs) -> str:
        """ Obtain symbol name in a single task.
        """

        map_index = kwargs["map_index"]

        name = _to_safe_name(symbols[map_index]["symbol"])

        return "".join([name, ".prefect"])

    with Flow(
        "Fetch data for GlobalAssets from Investing", result=result
    ) as flow_fetch_rawdata:

        symbol_list = Parameter("symbols", default=symbols)

        fetch_data_investing = Fetch_data_investing(
            checkpoint=True, target=_get_symbol)

        fetch_data_investing.map(symbol_list)

    return flow_fetch_rawdata
