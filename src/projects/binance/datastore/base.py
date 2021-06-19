from dataclasses import dataclass
import pandas as pd
from lib2.datastore import LocalDataStore as _LocalDataStore

not_proper_symbols = ["BTCSTUSDT"]


@dataclass(frozen=True)
class BinanceLocalDataStore(_LocalDataStore):

    path: str

    def list_symbols(self):

        df: pd.DataFrame = pd.read_hdf(self.path, key="close")

        total_symbols = df.columns.tolist()
        futures = [symbol for symbol in total_symbols if "_" in symbol]

        total_symbols = list(
            set(total_symbols) - set(futures) - set(not_proper_symbols)
        )

        total_symbols.sort()

        return total_symbols
