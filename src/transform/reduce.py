from typing import Tuple, List, Any
from prefect import task


@task
def concat(datas: List[Tuple[Any, str]]) -> dict:
    """Load raw data of investing.com from the datalake

    Args:
        datas (List[str]): list of tuple data. We assume tuple = (raw, meta).

    Returns:
        dict: raw data of the symbol
    """

    result = {}

    # Load and clean data
    for data in datas:
        print(data[1])
        result[data[1]] = data[0]

    return result
