from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

import pandas as pd
from sqlalchemy import Table, Select


@dataclass
class Feature(ABC):
    subject_table: Table
    configurations: dict
    requirements: dict

    @property
    @abstractmethod
    def feature_name(self) -> str:
        pass

    @abstractmethod
    def construct_query(self) -> Select:
        pass

    @abstractmethod
    def validate(self, retrieved_data: pd.DataFrame) -> bool:
        pass
