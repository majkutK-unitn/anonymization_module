from abc import ABC, abstractmethod

from typing import Tuple

from models.attribute import Attribute


class AbstractAPI(ABC):
    @abstractmethod
    def push_ecs(self, ecs: list) -> bool:
        pass

    @abstractmethod
    def get_document_count(self, attributes: dict[str, Attribute]) -> int:
        pass        
    
    @abstractmethod
    def get_attribute_min_max(self, attr_name: str, attributes: dict[str, Attribute]) -> Tuple[int,int]:
        pass
