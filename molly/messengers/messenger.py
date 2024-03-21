from abc import abstractmethod
from dataclasses import dataclass
from typing import Dict


@dataclass
class Messenger(ABC):
    credentials: Dict[str, str]

    @property
    @abstractmethod
    def messenger_name(self) -> str:
        pass

    @abstractmethod
    def send(self, message: str, destination: str):
        pass
