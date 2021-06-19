import pandas as pd

import prefect
from prefect import task

from .helper import CURRENT


@task(
    checkpoint=True,
    target="examples/raw_examples_{hash_value}.pkl",
    result=CURRENT,
)
def generate_raw_examples(
    features: pd.DataFrame,
    label: pd.Series,
) -> pd.DataFrame:

    logger = prefect.context.get("logger")

    logger.info("Start to create raw examples.")

    if not isinstance(label, pd.DataFrame):
        label = pd.DataFrame(label)

    # Generate examples from the features and the labels.
    examples = pd.merge(
        left=label,
        right=features,
        left_index=True,
        right_index=True,
        how="left",
    )

    logger.info("End of creating raw examples.")

    return examples
