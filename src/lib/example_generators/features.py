from typing import Tuple, List, Union, Any
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

import prefect
from prefect import task
from prefect.engine.results import LocalResult


CURRENT = LocalResult(dir="./datastore/")


@task(
    checkpoint=True,
    target="feature/{dfmeta[1]}/{parameters[hash_value_features]}.pkl",
    result=CURRENT,
)
def generate_features_byassets(
    dfmeta: Tuple[pd.DataFrame, str], feature_pipes: List[Any]
):
    """Generate features for each symbols.

    Args:
        dfmeta (Tuple[pd.DataFrame, str]): Dict of dataframe of all data used for creating feature and symbol.
        feature_pipes (List[str]): Feature pipeline defined in lib.compute_pipes

    Returns:
        pd.DataFrame: Dataframe of features for single asset.

    Note:
        - We allow many NaNs at this moment. We treat them in creating examples later.
    """

    df = dfmeta[0]
    symbol_name = dfmeta[1]

    results = []
    for pipe_cls in feature_pipes:

        dfmeta = (df, {"Symbol": symbol_name})

        feature: Union[pd.DataFrame, pd.Series] = pipe_cls.compute(dfmeta).rename(
            pipe_cls.name
        )

        results.append(feature)

    features = pd.concat(results, axis=1)

    return features


@task(
    checkpoint=True,
    target="features/{parameters[symbols]}_{parameters[hash_value_features]}.pkl",
    result=CURRENT,
)
def create_features(feature_byassets: List[pd.DataFrame]):

    logger = prefect.context.get("logger")

    logger.info("Start to create featrues.")

    result = []

    for feature in feature_byassets:

        result.append(feature)

    df = pd.concat(result, axis=0)

    df.index = [df.index, df["Symbol"]]
    df = df.rename(columns={"Symbol": "Symbol_category"})

    # Categorical to number
    cat_features = df.select_dtypes(include=object)

    df.loc[:, cat_features.columns] = cat_features.apply(_cat_to_num)

    df = df.sort_index(level=0)

    # Remove columns with inf.
    inf_col = df.describe().apply(lambda x: np.isinf(x.values).sum())
    df = df.loc[:, inf_col[(inf_col == 0)].index]

    logger.info("End of creating featrues.")

    return df


def _cat_to_num(x: pd.Series) -> np.array:

    le = LabelEncoder()

    le = le.fit(x.values)

    return le.transform(x)
