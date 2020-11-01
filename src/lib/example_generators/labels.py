from typing import Dict, List, Union
import pandas as pd

import prefect
from prefect import task

import lib.compute_pipes as cp
from .helper import CURRENT


@task(checkpoint=True, target="label/{dfmeta[1]}/{label_pipes}.pkl", result=CURRENT)
def generate_label_byassets(dfmeta: Dict[pd.DataFrame, str], label_pipes: List[str]):

    df = dfmeta[0]
    symbol_name = dfmeta[1]

    results = []

    # Pipes should be single label and single symbol.
    assert len(label_pipes) == 2

    for pipe in label_pipes:

        pipe_cls = getattr(cp, pipe)()

        pipe_cls.dfmeta = (df, {"symbol": symbol_name})

        label: Union[pd.DataFrame, pd.Series] = pipe_cls.compute()

        results.append(label)

    labels = pd.concat(results, axis=1)

    return labels.dropna()


@task(
    checkpoint=True,
    target="labels/labels_window_{window}_demean_{demean}.pkl",
    result=CURRENT,
)
def create_labels(
    label_byassets: List[pd.DataFrame], window: int = 30, demean=True
):

    logger = prefect.context.get("logger")

    logger.info("Start to create labels.")

    label_list = []

    for label in label_byassets:

        label_list.append(label)

    df = pd.concat(label_list, axis=0, sort=True)

    # To multindex.
    df.index = [df.index, df["symbol"]]
    df = df.sort_index(level=0)

    # Rename and drop symbol in columns.
    df = df.drop(columns="symbol")
    df.columns = ["label"]

    if demean:
        df = df.groupby(pd.Grouper(level=0)).apply(lambda x: x - x.mean())

    return df
