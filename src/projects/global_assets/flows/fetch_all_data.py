from typing import List
from prefect import Flow, Parameter
from prefect.engine.result import Result
from prefect.engine.results import LocalResult

from lib.extract import FetchInvesting, Reader
from lib.transform import CleanDataInvesting

__all__ = ["fetch_global_data", "clean_global_data"]


def fetch_global_data(symbols: List[dict], result: Result = LocalResult()) -> Flow:
    """ Fetch all data for global assets from investing.com in data lake.

    Args:
        symbols (List[dict]): List of dictionary. each dictionary have 'symbol', 'asset', 'country' key.
        result (Result): Result instance to write data.
    """

    with Flow("Fetch data for GlobalAssets from investing.com.",) as flow_fetch_rawdata:

        symbol_list = Parameter("symbols", default=symbols)

        fetch_data_investing = FetchInvesting(result=result)

        fetch_data_investing.map(symbol_list)

    return flow_fetch_rawdata


def clean_global_data(
    symbols: List[dict], input: Result = LocalResult(), output: Result = LocalResult(),
) -> Flow:
    """ Clean all raw data for global assets and store in data warehouse.

    Args:
        symbols (List[dict]): List of dictionary. each dictionary have 'symbol', 'asset', 'country' key.
        input_result (Result): Result instance to read data.
        output_result (Result): Result instance to write data.
    """

    with Flow("Clean data for GlobalAssets.",) as flow_fetch_rawdata:

        symbol_list = Parameter("symbols", default=symbols)

        reader = Reader(result=input)

        raw_datas = reader.map(symbol_list)

        cleaner = CleanDataInvesting(result=output)
        cleaner.map(raw_datas)

    return flow_fetch_rawdata
