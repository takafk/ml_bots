from typing import Tuple, Union, List
import pandas as pd

from core import Compute
from prefect import task

__all__ = ["generate_features"]


@task(checkpoint=False)
def generate_features(dfmeta: Tuple[pd.DataFrame, dict], pipes: List[Compute]):

    meta = dfmeta[1]

    results = []
    for pipe in pipes:

        pipe.dfmeta = dfmeta

        feature: Union[pd.DataFrame, pd.Series] = pipe.compute()

        results.append(feature)

    features = pd.concat(results, axis=1)

    return (features.dropna(), meta)
