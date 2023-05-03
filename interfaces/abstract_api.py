from abc import ABC, abstractmethod


class AbstractAPI(ABC):
    @abstractmethod
    def push_ecs(ecs: list) -> bool:
        pass