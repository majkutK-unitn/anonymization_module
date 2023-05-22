from abc import ABC, abstractmethod

from typing import Tuple

from models.attribute import Attribute
from models.partition import Partition


class AbstractAPI(ABC):
    @abstractmethod
    def push_partitions(self, partitions: list[Partition]):
        pass

    @abstractmethod
    def get_document_count(self, attributes: dict[str, Attribute]) -> int:
        pass        
    
    @abstractmethod
    def get_attribute_min_max(self, attr_name: str, attributes: dict[str, Attribute]) -> Tuple[int,int]:
        pass
