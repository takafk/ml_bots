from typing import Tuple, Any

from prefect import Task
from prefect.engine.results import LocalResult
from prefect.engine.result import Result
from prefect.engine.serializers import JSONSerializer

from transform import _to_safe_name

__all__ = ["Writer"]


class Writer(Task):
    """Read raw data from a Result.
    """

    def __init__(
        self, result: Result = LocalResult(serializer=JSONSerializer), *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.result = result

    def run(self, datas: Tuple[str, Any]) -> tuple:

        name = datas[0]
        data = datas[1]

        # Write raw data
        self.result.location = f"{name}.prefect"

        written_result = self.result.write(data)

        return(written_result)

    def _get_name(self, name: str, **kwargs) -> str:

        name = _to_safe_name(name)

        return name
