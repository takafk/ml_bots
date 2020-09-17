import abc
from typing import Optional
from dataclasses import dataclass


@dataclass  # type: ignore
class Compute(metaclass=abc.ABCMeta):

    name: Optional[str] = None

    def __post_init__(self):
        if self.name is None:
            self.name = self.__class__.__name__

    @abc.abstractmethod
    def inputs(self):
        # Define imputs for this computation.
        raise NotImplementedError()

    @abc.abstractmethod
    def compute(self):
        # Computing based on the inputs.
        raise NotImplementedError()
