from typing import Dict, Union

from prefect import Task
from prefect.engine.results import LocalResult
from prefect.engine.result import Result
from prefect.engine.serializers import JSONSerializer

from transform import _to_safe_name

__all__ = ["Reader"]


class Reader(Task):
    """Read raw data from a Result.
    """

    def __init__(
        self, result: Result = LocalResult(serializer=JSONSerializer), *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.result = result

    def run(self, symbols: Union[Dict[str, str], str]) -> tuple:
        """[summary]

        Args:
            symbols (Dict[str, str]): symbols to load. key is 'symbol'.

        Returns:
            Dict[str, Any]: [description]
        """

        if isinstance(symbols, Dict):
            symbol = _to_safe_name(symbols["symbol"])
        else:
            symbol = symbols

        # Read raw data
        raw = self.result.read(location=f"{symbol}.prefect").value

        return (raw, symbols)
