from abc import abstractmethod

from typing import Tuple

from models.partition import Partition

from interfaces.abstract_api import AbstractAPI


class MondrianAPI(AbstractAPI):
    def __init__(self):
        pass

    @abstractmethod
    def get_value_to_split_at_and_next_unique_value(self,  attr_name: str, partition: Partition) -> Tuple[int, int]:
        pass