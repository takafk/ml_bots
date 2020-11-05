import pandas as pd

import prefect
from prefect import task

from .helper import CURRENT


@task(checkpoint=True, target="examples/raw_examples_{hash_value}.pkl", result=CURRENT)
def generate_raw_examples(
    features: pd.DataFrame, labels: pd.DataFrame, min_symbols: int
) -> pd.DataFrame:

    logger = prefect.context.get("logger")

    logger.info("Start to create raw examples.")

    if not isinstance(labels, pd.DataFrame):
        labels = pd.DataFrame(labels)

    # Generate examples from the features and the labels.
    examples = pd.merge(
        left=labels, right=features, left_index=True, right_index=True, how="left"
    )

    # Remove index when we don't have information of all assets. (related to min universe)
    num_of_assets = examples.groupby(examples.index.get_level_values(0)).apply(
        lambda x: len(x)
    )
    target_index = num_of_assets[num_of_assets >= min_symbols].index
    examples = examples.loc[target_index, :]

    logger.info("End of creating raw examples.")

    return examples
