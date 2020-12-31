import abc
from typing import Optional
from dataclasses import dataclass


@dataclass(frozen=True)  # type: ignore
class Compute(metaclass=abc.ABCMeta):

    name: Optional[str] = None

    def __post_init__(self):
        if self.name is None:
            object.__setattr__(self, "name", self.__class__.__name__)

    @abc.abstractmethod
    def inputs(self, dfmeta):
        # Define imputs for this computation.
        raise NotImplementedError()

    @abc.abstractmethod
    def compute(self, dfmeta: tuple):
        # Computing based on the inputs.
        raise NotImplementedError()
