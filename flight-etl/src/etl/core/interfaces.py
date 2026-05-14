import abc
from typing import Any

import pandas as pd


class BaseExtract(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def extract(self):
        raise NotImplementedError


class BaseTransform(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def transform(self):
        raise NotImplementedError


# class BaseLoad(metaclass=abc.ABCMeta):
#     @classmethod
#     def __subclasshook__(cls, subclass: type) -> bool:
#         return hasattr(subclass, "load") and callable(subclass.load)

#     @abc.abstractmethod
#     def load(self, data: Any, filepath: str):
#         raise NotImplementedError
