from interfaces.abstract_api import AbstractAPI
from models.partition import Partition


class MondrianAPI(AbstractAPI):
    def __init__(self):
        pass

    def push_ecs(ecs: list) -> bool:
        pass

    def get_median(partition: Partition, qid) -> int|float:
        pass

    def check_ec_validity(partition_candidate) -> bool:
        pass