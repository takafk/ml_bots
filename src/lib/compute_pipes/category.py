from dataclasses import dataclass, field
import pandas as pd

from core import Compute

__all__ = [
    "Symbol",
    "Country",
    "Asset",
]


@dataclass
class Symbol(Compute):

    dfmeta: tuple = field(default_factory=tuple)
    name: str = "symbol"

    def inputs(self) -> tuple:
        return self.dfmeta

    def compute(self) -> pd.Series:

        dfmeta = self.inputs()

        data: pd.DataFrame = dfmeta[0]
        metadata: dict = dfmeta[1]

        if "symbol" not in metadata.keys():
            raise ValueError("Symbol should be given in metadata.")

        result = pd.Series([metadata["symbol"] for i in range(len(data))])
        result.index = data.index

        return result.rename(self.name)


@dataclass
class Country(Compute):

    dfmeta: tuple = field(default_factory=tuple)
    name: str = "country"

    def inputs(self) -> tuple:
        return self.dfmeta

    def compute(self) -> pd.Series:

        dfmeta = self.inputs()

        data: pd.DataFrame = dfmeta[0]
        metadata: dict = dfmeta[1]

        if "country" not in metadata.keys():
            raise ValueError("Country should be given in metadata.")

        result = pd.Series([metadata["country"] for i in range(len(data))])
        result.index = data.index

        return result.rename(self.name)


@dataclass
class Asset(Compute):

    dfmeta: tuple = field(default_factory=tuple)
    name: str = "asset"

    def inputs(self) -> tuple:
        return self.dfmeta

    def compute(self) -> pd.Series:

        dfmeta = self.inputs()

        data: pd.DataFrame = dfmeta[0]
        metadata: dict = dfmeta[1]

        if "asset" not in metadata.keys():
            raise ValueError("Country should be given in metadata.")

        result = pd.Series([metadata["asset"] for i in range(len(data))])
        result.index = data.index

        return result.rename(self.name)
