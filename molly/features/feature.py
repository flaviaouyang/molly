from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import Table


@dataclass
class Feature(ABC):
    subject_table: Table
    configurations: dict
    requirements: dict

    @abstractmethod
    def construct_query(self):
        pass

    @abstractmethod
    def validate(self, retrieved_data: pd.DataFrame) -> bool:
        pass
