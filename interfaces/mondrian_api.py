from typing import Tuple

from models.partition import Partition
from interfaces.abstract_api import AbstractAPI


class MondrianAPI(AbstractAPI):
    def __init__(self):
        pass

    def get_median(self, partition: Partition, qid_index: int) -> Tuple[str, str, str, str]:
        pass

    def get_number_of_nodes_covered(self, partition: Partition, qid_index: int, values: list[str]):
        pass

    def check_ec_validity(self, partition_candidate) -> bool:
        pass