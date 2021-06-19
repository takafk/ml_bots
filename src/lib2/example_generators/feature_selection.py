import pandas as pd

from prefect import task


@task(checkpoint=False)
def ic_selection(
    raw_examples: pd.DataFrame, num_of_features: int = 6
) -> pd.DataFrame:
    """Feature selection by historical cross-sectional IC.

    Args:
        raw_examples (pd.DataFrame): raw examples (labels + features) to select features.
        num_of_features (int, optional): Number of the returned features. Defaults to 6.

    Returns:
        pd.DataFrame: examples with selected features.

    Notes:
        - We cannot use Dask for this operation because Dask use much memory to compute correlation.
        For details, see this PR.
    """
    num_examples = raw_examples.select_dtypes(include=float)

    # Compute cross-sectional correlations.
    cross_sectional_corr = num_examples.groupby(pd.Grouper(level=0)).corr()[
        "label"
    ]

    cross_sectional_corr = cross_sectional_corr.unstack()

    # Compute cumulative cross-sectional correlations.
    # We should use mean, not sum, to chose features whose relations to label are consistent.
    cumulative_corr = (
        cross_sectional_corr.loc[:, cross_sectional_corr.columns != "label"]
        .mean()
        .sort_values()
    )

    # Select features in the top and bottom groups.
    selected_features = list(
        cumulative_corr[:num_of_features // 2].index
    ) + list(cumulative_corr[-num_of_features // 2:].index)

    return raw_examples[["label"] + selected_features]
