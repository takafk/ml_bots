from typing import Tuple, List, Any
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

import prefect
from prefect import task
from prefect.engine.results import LocalResult


CURRENT = LocalResult(dir="./datastore/")


@task(
    checkpoint=True,
    target="feature/{feature_pipe.name}/{parameters[hash_value_universe]}_{start_dt}_{end_dt}.pkl",
    result=CURRENT,
)
def generate_feature(
    dsmeta: Tuple[str, List[str]], feature_pipe: Any, start_dt: str, end_dt: str
):
    """Generate features for each symbols.

    Args:
        dfmeta (Tuple[str, List[str]]): Tuple of datastore and symbols.
        feature_pipe (Any): Feature pipeline defined in lib2.compute_factory
        start_dt: start day of feature.
        end_dt: end day of feature.

    Returns:
        pd.DataFrame: Dataframe of feature.

    Note:
        - We allow many NaNs at this moment. We treat them in creating examples later.
    """

    # index = timestamp, columns = symbols
    feature: pd.DataFrame = feature_pipe.compute(dsmeta)

    feature_multi = feature.stack().sort_index().rename(feature_pipe.name)

    # Limit the lenghth of the data.
    # Without this limitation, we cannot create dataframe of features in generate_features
    # due to the error, "_pickle.UnpicklingError: pickle data was truncated".
    feature_multi = feature_multi[
        (feature_multi.index.get_level_values(0) >= start_dt)
        & (feature_multi.index.get_level_values(0) <= end_dt)
    ]

    return feature_multi


@task(
    checkpoint=True,
    target="features/{parameters[hash_value_features]}.pkl",
    result=CURRENT,
)
def generate_features(features: List[pd.DataFrame]):
    """Generate features for each symbols.

    Args:
        features: List of features.

    Returns:
        pd.DataFrame: Dataframe of features with multiIndex.

    Note:
        - We allow many NaNs at this moment. We treat them in creating examples later.
    """

    logger = prefect.context.get("logger")

    logger.info("Start to create featrues.")

    results = []

    for feature_multi in features:

        assert (
            len(feature_multi.dropna()) != 0
        ), f"{feature_multi.name} of values are all None."

        results.append(feature_multi)

    df = pd.concat(results, axis=1).sort_index(level=0)

    # Categorical to number
    cat_features = df.select_dtypes(include=object)

    df.loc[:, cat_features.columns] = cat_features.apply(_cat_to_num)

    # Remove columns with inf.
    inf_col = df.describe().apply(lambda x: np.isinf(x.values).sum())
    df = df.loc[:, inf_col[(inf_col == 0)].index]

    logger.info("End of creating featrues.")

    return df


def _cat_to_num(x: pd.Series) -> pd.Series:

    # For several cases, x can have NaN in the values.
    index_org = x.index

    x_notnull = x.dropna()
    index = x_notnull.index

    le = LabelEncoder()
    le = le.fit(x_notnull.values)

    cat_series_transformed = pd.Series(data=le.transform(x_notnull), index=index)

    return cat_series_transformed.reindex(index_org)
