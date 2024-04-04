from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Union

import pandas as pd
from sqlalchemy import Select, Table


@dataclass
class Feature(ABC):
    subject_table: Table
    configurations: dict
    requirements: dict

    def __post_init__(self):
        self.__query_info = None
        self.__validation_result = None

    @property
    @abstractmethod
    def feature_name(self) -> str:
        pass

    @property
    def query_info(self) -> str:
        if self.__query_info is None:
            raise ValueError(
                f"No query information available. Try running construct query first."
            )
        return self.__query_info

    @property
    def validation_result(self):
        if self.__validation_result is None:
            raise ValueError(
                f"No validation result available. Try running validate first."
            )
        return self.__validation_result

    @abstractmethod
    def construct_query(self) -> Select:
        pass

    @abstractmethod
    def validate(
        self, retrieved_data: Union[pd.DataFrame, Iterable[pd.DataFrame]]
    ) -> bool:
        pass

    @abstractmethod
    def describe(self) -> str:
        pass
