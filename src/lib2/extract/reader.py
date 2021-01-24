from typing import Dict, Tuple, Union, Any
import pandas as pd

from prefect import Task
from prefect.engine.results import LocalResult
from prefect.engine.result import Result

__all__ = ["Reader"]


class Reader(Task):
    """Read pkl data from a Result."""

    def __init__(self, result: Result = LocalResult(), *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.result = result
        self.checkpoint = False

    def run(self, keys: Union[Dict[str, str], str]) -> Tuple[pd.DataFrame, Any]:
        """
        Args:
            keys (Dict[str, str]): meta data for target symbol.
            If dict, there must be the key 'symbol'.

        Returns:
            Tuple[pd.DataFrame, dict]
        """

        if isinstance(keys, dict):
            symbol = keys["symbol"]
        else:
            symbol = keys

        # Read raw data
        raw = self.result.read(location=f"{symbol}.pkl").value

        return (raw, symbol)
