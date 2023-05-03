from abc import ABC, abstractmethod


class AbstractAlgorithm(ABC):
    @abstractmethod
    def anonymize(config) -> bool:
        pass