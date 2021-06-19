from typing import Dict, List, Union, Any
import pandas as pd

import prefect
from prefect import task

from .helper import CURRENT


@task(
    checkpoint=True,
    target="label/{dfmeta[1]}/{parameters[hash_value_labels]}.pkl",
    result=CURRENT,
)
def generate_label_byassets(
    dfmeta: Dict[pd.DataFrame, str], label_pipes: List[Any]
):

    df = dfmeta[0]
    symbol_name = dfmeta[1]

    results = []

    # Pipes should be single label and single symbol.
    assert len(label_pipes) == 2

    for pipe_cls in label_pipes:

        dfmeta = (df, {"Symbol": symbol_name})

        label: Union[pd.DataFrame, pd.Series] = pipe_cls.compute(
            dfmeta
        ).rename(pipe_cls.name)

        results.append(label)

    labels = pd.concat(results, axis=1)

    return labels.dropna()


@task(
    checkpoint=True,
    target="labels/{parameters[symbols]}_demean_{demean}_{parameters[hash_value_labels]}.pkl",
    result=CURRENT,
)
def create_labels(label_byassets: List[pd.DataFrame], demean=True):

    logger = prefect.context.get("logger")

    logger.info("Start to create labels.")

    label_list = []

    for label in label_byassets:

        label_list.append(label)

    df = pd.concat(label_list, axis=0, sort=True)

    # To multindex.
    df.index = [df.index, df["Symbol"]]
    df = df.sort_index(level=0)

    # Rename and drop symbol in columns.
    df = df.drop(columns="Symbol")
    df.columns = ["label"]

    if demean:
        df = df.groupby(pd.Grouper(level=0)).apply(lambda x: x - x.mean())

    logger.info("End of creating labels.")

    return df
