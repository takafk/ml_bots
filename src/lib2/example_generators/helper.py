import pandas as pd
import numpy as np

from prefect import task
from prefect.engine.results import LocalResult

CURRENT = LocalResult(dir="./datastore/")


@task(
    checkpoint=True,
    target="examples/examples_{parameters[hash_value]}.pkl",
    result=CURRENT,
)
def post_label_transform(
    selected_examples: pd.DataFrame, is_classification: bool = True
) -> pd.DataFrame:

    if is_classification:

        # relative value + qcut (treatment for imbalanced label, very important)
        CLASS = 10
        labels_cat, bins = pd.qcut(
            selected_examples["label"], CLASS, labels=False, retbins=True
        )
        selected_examples.loc[pd.IndexSlice[:, :], "label"] = labels_cat.values

        x1 = np.roll(bins, -1)
        ave_pred = (bins + x1) / 2
        ave_pred = ave_pred[:-1]

        CURRENT.location = "examples/ave_pred.pkl"
        CURRENT.write(ave_pred)

    return selected_examples
