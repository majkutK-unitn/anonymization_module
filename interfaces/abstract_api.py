from abc import ABC, abstractmethod


class AbstractAPI(ABC):
    @abstractmethod
    def push_ecs(self, ecs: list) -> bool:
        pass