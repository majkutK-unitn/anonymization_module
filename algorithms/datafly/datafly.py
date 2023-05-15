from interfaces.abstract_algorithm import AbstractAlgorithm
from interfaces.datafly_api import DataflyAPI

from models.gentree import GenTree
from models.partition import Partition

from utils.read_gen_hierarchies import read_gen_hierarchies_from_config_v2


class Datafly(AbstractAlgorithm):
    def __init__(self, db_connector: DataflyAPI, config: dict[str, int|dict]):
        self.db_connector = db_connector
        self.k: int = config["k"]
        self.qid_names: list[str] = config["attributes"].keys()        
        self.partitions : list[Partition] = []        
        self.size_of_dataset: int = None
        self.numerical_attr_config = {}
        self.categorical_attr_config = {}

        for key, value in config['attributes'].items():
            if "tree" in value:
                if value["datafly_init_level"] != 0:
                    self.categorical_attr_config[key] = value
            else:
                self.numerical_attr_config[key] = value

        self.gen_hiers: dict[str, GenTree] = read_gen_hierarchies_from_config_v2(self.categorical_attr_config)
        

    # (1) Get a dict of attributes: what generalization to use (num > how many buckets: cat > what level)
    # (2) Create an Attribute of each gen_value
    # (3) Iterate through all attributes and combine them one by one (age x edu_num: (age, edu_num) x race, (age, edu_num, race) x sex)
    def generate_initial_partitions():
        pass
    
    def run(self) -> bool:
        pass
    
    def calculate_ncp(self) -> float:
        pass