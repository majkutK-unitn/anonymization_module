from abc import ABC, abstractmethod

from interfaces.abstract_api import AbstractAPI


class AbstractAlgorithm(ABC):
    @abstractmethod
    def __init__(self, db_connector: AbstractAPI, config):
        pass

    @abstractmethod
    def run(self) -> bool:
        pass

    @abstractmethod
    def calculate_ncp(self) -> float:
        pass