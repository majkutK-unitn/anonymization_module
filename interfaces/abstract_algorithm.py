from abc import ABC, abstractmethod

from interfaces.abstract_api import AbstractAPI


class AbstractAlgorithm(ABC):
    @abstractmethod
    def __init__(self, db_connector: AbstractAPI):
        pass

    @abstractmethod
    def run(self, config: dict[str, int|dict]) -> bool:
        pass

    @abstractmethod
    def calculate_ncp(self) -> float:
        pass