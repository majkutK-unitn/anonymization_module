from abc import abstractmethod

from typing import Tuple

from models.attribute import Attribute
from models.partition import Partition

from interfaces.abstract_api import AbstractAPI


class DataflyAPI(AbstractAPI):
    def __init__(self):
        pass
        
    @abstractmethod
    def spread_attribute_into_uniform_buckets(attr_name: str):
        pass