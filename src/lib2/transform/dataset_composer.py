from typing import List
from dataclasses import dataclass, field
import pandas as pd
import pickle


@dataclass(frozen=True)
class DatastoreComposer:
    """Composer of HDF5 Datastore.

    Notes:
        - Until now (01/23/2021), we load raw data by symbol. However, we cannot create
        cross-sectional features in this architecture.
        To create the kind of features, we decied to save the data by field name such as open.
        For this purpose, it would be better to use HDF5 store than json or pkl
        and this class is created on this purpose.

    Args:
        path(str): path to read pickle data.
        symbols(List[str]): list of symbols used for composing this datastore.
        data_names(List[str]): list of data names such as ohlc and lsratio.
        start_dt(str): start date/
        end_dt(str): end date.
    Returns:
    """

    path: str
    symbols: list = field(default_factory=List[str])
    data_names: list = field(default_factory=List[str])
    start_dt: str = field(default_factory=str)
    end_dt: str = field(default_factory=str)

    def compose_data(self, path: str, symbols: List[str], data_name: str):

        result = []

        for symbol in symbols:

            with open(f"{path}/{symbol}_{data_name}.pkl", "rb") as f:
                # Assuming that the input data is dataframe (index = timestamp)
                # and all datas are cleaned appropriately.
                df = pickle.load(f)

            df.index.name = "timestamp"
            df = df[(df.index >= self.start_dt) & (df.index <= self.end_dt)]

            df["symbol"] = symbol
            result.append(df)

        df_field = pd.concat(result, axis=0).sort_index()

        df_field = df_field.reset_index().set_index(["timestamp", "symbol"])

        return df_field

    def write_hdf5_bycolumn(self, path: str, df: pd.DataFrame):
        for col in df.columns:
            df.loc[:, col].unstack().to_hdf(f"{path}/datastore.h5", key=col, mode="a")

    def compose_datastore(self):

        for data_name in self.data_names:

            df_field = self.compose_data(self.path, self.symbols, data_name)

            self.write_hdf5_bycolumn(self.path, df_field)
