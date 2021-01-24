from dataclasses import dataclass
import pandas as pd

from core import Compute


@dataclass(frozen=True)
class Symbol(Compute):
    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta) -> pd.Series:

        inputs = self.inputs(dfmeta)

        data: pd.Series = inputs[0]
        metadata: dict = inputs[1]

        if "Symbol" not in metadata.keys():
            raise ValueError("Symbol should be given in metadata.")

        result = pd.Series([metadata["Symbol"] for i in range(len(data))])
        result.index = data.index

        return result


@dataclass(frozen=True)
class Country(Compute):
    def inputs(self, dfmeta):
        return dfmeta

    def compute(self, dfmeta) -> pd.Series:

        inputs = self.inputs(dfmeta)

        data: pd.Series = inputs[0]
        metadata: dict = inputs[1]

        if "Country" not in metadata.keys():
            raise ValueError("Country should be given in metadata.")

        result = pd.Series([metadata["Country"] for i in range(len(data))])
        result.index = data.index

        return result
