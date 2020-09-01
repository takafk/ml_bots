from typing import Union
import json
import pandas as pd
from prefect import Task


class DataTask(Task):
    """ Task to manipulate data.

    In Task on Perfect, we need to define a task to save data.
    However, it increase the number of Tasks.

    To avoid this, we add the method 'to_json' to Task.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_json(self, output: Union[pd.DataFrame, dict], name: str, storage: str):

        file_name = "".join([storage, name, ".json"])

        if isinstance(output, pd.DataFrame):
            output.to_json(file_name)
        else:
            with open(storage, "w") as f:
                json.dump(file_name, f)
