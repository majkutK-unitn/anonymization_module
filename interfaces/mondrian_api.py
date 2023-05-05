from abc import abstractmethod

from typing import Tuple

from models.attribute import Attribute
from models.partition import Partition

from interfaces.abstract_api import AbstractAPI


class MondrianAPI(AbstractAPI):
    def __init__(self):
        pass

    @abstractmethod
    def get_document_count(self, attributes: dict[str, Attribute] = None) -> int:
        pass        

    @abstractmethod
    def get_attribute_median_and_next_value(self, attributes: dict[str, Attribute], attr_name: str) -> Tuple[str, str]:
        pass

    @abstractmethod
    def get_attribute_min_max(self, attr_name: str) -> Tuple[int,int]:
        pass
