import numpy as np
import pandas as pd


def split_sequences_chart(df: pd.DataFrame, lookback_window: int):
    """Resahpe dataframe for CNN chart patters.

    Args:
        df (pd.DataFrame): dataframe of label and features (index = [timestamp, symbol])
        lookback_window (int): length of window for CNN.

    Returns:
        [type]: [description]
    """

    X, y = list(), list()
    indices = []

    assert df.index.names == ["timestamp", "symbol"]

    swaped = df.swaplevel().sort_index()
    columns = swaped.columns.tolist()
    symbols = list(set(swaped.index.get_level_values(0)))

    if "label" in columns:
        columns.remove("label")
        swaped = swaped[columns + ["label"]]

        for symbol in symbols:

            df_symbol = swaped.loc[symbol]

            for i in range(len(df_symbol)):

                # check if we are beyond the dataset
                if i + lookback_window <= len(df_symbol):

                    X.append(df_symbol.iloc[i : i + lookback_window, :-1].T.values)
                    y.append(df_symbol.iloc[i : i + lookback_window, -1].values[-1])
                    indices.append(
                        [df_symbol.iloc[i : i + lookback_window, -1].index[-1], symbol]
                    )

    else:
        for symbol in symbols:

            df_symbol = swaped.loc[symbol]

            for i in range(len(df_symbol)):

                # check if we are beyond the dataset
                if i + lookback_window <= len(df_symbol):

                    X.append(df_symbol.iloc[i : i + lookback_window, :].T.values)
                    indices.append(
                        [df_symbol.iloc[i : i + lookback_window, -1].index[-1], symbol]
                    )

    return np.array(X), np.array(y), indices
