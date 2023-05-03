from typing import Tuple

from algorithms.mondrian.models.partition import Partition
from interfaces.abstract_api import AbstractAPI


class MondrianAPI(AbstractAPI):
    def __init__(self):
        pass

    def push_ecs(self, ecs: list) -> bool:
        pass

    def get_median(self, partition: Partition, qid_index: int) -> Tuple[str, str, str, str]:
        pass

    def check_ec_validity(self, partition_candidate) -> bool:
        pass