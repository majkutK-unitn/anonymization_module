from abc import ABC, abstractmethod

from interfaces.abstract_api import AbstractAPI
from models.config import Config
from models.partition import Partition


class AbstractAlgorithm(ABC):
    final_partitions : list[Partition]
    
    @abstractmethod
    def __init__(self, db_connector: AbstractAPI):
        pass

    @abstractmethod
    def run(self, config: dict[str, int|dict]):
        pass

    
    def get_normalized_width(self, partition: Partition, qid_name: str) -> float:    
        """ Return Normalized width of partition """        

        return partition.attributes[qid_name].width * 1.0 / len(Config.attr_metadata[qid_name])


    def calculate_ncp(self):
        ncp = 0.0

        for partition in self.final_partitions:
            ncp_partiton = 0.0
            
            for attr_name in Config.qid_names:
                ncp_partiton += self.get_normalized_width(partition, attr_name)

            ncp_partiton *= partition.count
            ncp += ncp_partiton
        
        ncp /= len(Config.qid_names)
        ncp /= Config.size_of_dataset
        ncp *= 100

        return ncp