from typing import Tuple, List, Any
import pandas as pd
from prefect import task
from prefect.engine.results import LocalResult
from prefect.engine.result import Result
from core import DataTask


@task(checkpoint=True)
def concat_dict(datasets: List[Tuple[Any, str]]) -> dict:
    """Concat data and return dict.

    Args:
        datasets (List[str]): list of tuple data. We assume tuple = (raw, meta).

    Returns:
        dict: raw data of the symbol
    """

    result = {}

    # Load and concat dataset.
    for dataset in datasets:

        data = dataset[0]
        meta = dataset[1]

        result[meta["symbol"]] = data[0]

    return result


class Concat_dfmeta(DataTask):
    """[summary]
    """

    def __init__(
        self,
        result: Result = LocalResult(),
        target: str = "{task_name}.pkl",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.result = result
        self.target = target

    def run(
        self,
        datasets: List[Tuple[pd.DataFrame, str]],
        axis: int = 0,
        to_multi: bool = True,
    ) -> pd.DataFrame:
        """[summary]

        Args:
            datasets (List[Tuple[pd.DataFrame, str]]): [description]
            axis (int, optional): [description]. Defaults to 0.
            to_multi (bool, optional): [description]. Defaults to True.

        Returns:
            pd.DataFrame: [description]
        """

        result = []

        # Load and concat dataset.
        for dataset in datasets:

            result.append(dataset[0])

        results: pd.DataFrame = pd.concat(result, sort=False, axis=axis)

        if to_multi:
            assert "symbol" in results.columns
            results.index = [results.index, results["symbol"]]

        return results
